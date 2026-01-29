package metha

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jinzhu/now"
	log "github.com/sirupsen/logrus"
)

// Day has 24 hours.
const Day = 24 * time.Hour

var (
	// BaseDir is where all data is stored.
	BaseDir   = filepath.Join(UserHomeDir(), ".cache", "metha")
	fnPattern = regexp.MustCompile("(?P<Date>[0-9]{4,4}-[0-9]{2,2}-[0-9]{2,2})-[0-9]{8,}.xml(.gz)?$")

	// ErrAlreadySynced signals completion.
	ErrAlreadySynced = errors.New("already synced")
	// ErrInvalidEarliestDate for unparsable earliest date.
	ErrInvalidEarliestDate = errors.New("invalid earliest date")
)

type Harvester interface {
	Run() error
	Files() []string
	Dir() string
}

// PrependSchema prepends http, if its missing.
func PrependSchema(s string) string {
	if !strings.HasPrefix(s, "http") {
		return fmt.Sprintf("http://%s", s)
	}
	return s
}

type Config struct {
	BaseURL                    string
	Format                     string
	Set                        string
	From                       string
	Until                      string
	MaxRequests                int
	DisableSelectiveHarvesting bool
	CleanBeforeDecode          bool
	IgnoreHTTPErrors           bool
	MaxEmptyResponses          int
	SuppressFormatParameter    bool
	HourlyInterval             bool
	DailyInterval              bool
	ExtraHeaders               http.Header
	KeepTemporaryFiles         bool
	IgnoreUnexpectedEOF        bool
	Delay                      time.Duration
	MaxRetries                 int           // Maximum number of retry attempts
	RetryDelay                 time.Duration // Delay between retries
	RetryBackoff               float64       // Multiplier for delay between retries (e.g., 2.0 for exponential backoff)
}

// Harvest contains parameters for mass-download. MaxRequests and
// CleanBeforeDecode are switches to handle broken token implementations and
// funny chars in responses. Some repos do not support selective harvesting
// (e.g. zvdd.org/oai2). Set "DisableSelectiveHarvesting" to try to grab
// metadata from these repositories. From and Until must always be given with
// 2006-01-02 layout. TODO(miku): make zero type work (lazily run identify).
type Harvest struct {
	Config *Config
	Client *Client

	// XXX: Lazy via sync.Once?
	Identify *Identify
	Started  time.Time
	// Protects the rare case, where we are in the process of renaming
	// harvested files and get a termination signal at the same time.
	sync.Mutex
}

// NewHarvest creates a new harvest. A network connection will be used for an initial Identify request.
func NewHarvest(baseURL string) (*Harvest, error) {
	h := Harvest{Config: &Config{
		BaseURL:      baseURL,
		MaxRetries:   3,
		RetryDelay:   10 * time.Second,
		RetryBackoff: 2.0,
	}}
	if err := h.identify(); err != nil {
		return nil, err
	}
	return &h, nil
}

// Dir returns the absolute path to the harvesting directory.
func (h *Harvest) Dir() string {
	data := []byte(h.Config.Set + "#" + h.Config.Format + "#" + h.Config.BaseURL)
	return filepath.Join(BaseDir, base64.RawURLEncoding.EncodeToString(data))
}

// Files returns all files for a given harvest, without the temporary files.
func (h *Harvest) Files() []string {
	return MustGlob(filepath.Join(h.Dir(), "*.xml.gz"))
}

// mkdirAll creates necessary directories.
func (h *Harvest) mkdirAll() error {
	if _, err := os.Stat(h.Dir()); os.IsNotExist(err) {
		if err := os.MkdirAll(h.Dir(), 0755); err != nil {
			return err
		}
	}
	return nil
}

// dateLayout converts the repository endpoints advertised granularity to Go
// date format strings.
func (h *Harvest) dateLayout() string {
	switch h.Identify.Granularity {
	case "YYYY-MM-DD":
		return "2006-01-02"
	case "YYYY-MM-DDThh:mm:ssZ":
		return "2006-01-02T15:04:05Z"
	}
	return ""
}

// Run starts the harvest.
func (h *Harvest) Run() error {
	log.WithFields(log.Fields{
		"baseURL":                    h.Config.BaseURL,
		"format":                     h.Config.Format,
		"set":                        h.Config.Set,
		"from":                       h.Config.From,
		"until":                      h.Config.Until,
		"disableSelectiveHarvesting": h.Config.DisableSelectiveHarvesting,
		"hourlyInterval":             h.Config.HourlyInterval,
		"dailyInterval":              h.Config.DailyInterval,
		"dir":                        h.Dir(),
	}).Info("Starting harvest")

	if err := h.mkdirAll(); err != nil {
		log.WithFields(log.Fields{
			"dir":   h.Dir(),
			"error": err.Error(),
		}).Error("Failed to create directory")
		return err
	}

	h.setupInterruptHandler()
	h.Started = time.Now()

	log.WithFields(log.Fields{
		"started": h.Started,
	}).Debug("Harvest initialized")

	return h.run()
}

// temporaryFiles lists all temporary files in the harvesting dir.
func (h *Harvest) temporaryFiles() []string {
	return MustGlob(filepath.Join(h.Dir(), "*.xml-tmp*"))
}

// temporaryFilesSuffix list all temporary files in the harvesting dir having a suffix.
func (h *Harvest) temporaryFilesSuffix(suffix string) []string {
	return MustGlob(filepath.Join(h.Dir(), fmt.Sprintf("*.xml%s", suffix)))
}

// cleanupTemporaryFiles will remove all temporary files in the harvesting dir.
func (h *Harvest) cleanupTemporaryFiles() error {
	tempFiles := h.temporaryFiles()

	if h.Config.KeepTemporaryFiles {
		log.WithFields(log.Fields{
			"count": len(tempFiles),
			"dir":   h.Dir(),
		}).Info("Keeping temporary files")
		return nil
	}

	log.WithFields(log.Fields{
		"count": len(tempFiles),
		"dir":   h.Dir(),
	}).Debug("Cleaning up temporary files")

	for _, filename := range tempFiles {
		if err := os.Remove(filename); err != nil {
			if e, ok := err.(*os.PathError); ok && e.Err == syscall.ENOENT {
				log.WithFields(log.Fields{
					"filename": filename,
				}).Debug("File already removed")
				continue
			}
			log.WithFields(log.Fields{
				"filename": filename,
				"error":    err.Error(),
			}).Error("Failed to remove temporary file")
			return err
		}
		log.WithFields(log.Fields{
			"filename": filename,
		}).Debug("Removed temporary file")
	}

	log.WithFields(log.Fields{
		"count": len(tempFiles),
	}).Debug("Temporary files cleanup completed")

	return nil
}

// setupInterruptHandler will cleanup, so we can CTRL-C or kill savely.
func (h *Harvest) setupInterruptHandler() {
	log.Debug("Setting up interrupt handler")

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)

	go func() {
		<-sigc
		log.WithFields(log.Fields{
			"dir": h.Dir(),
		}).Info("Interrupt received, waiting for any rename to finish...")

		h.Lock()
		defer h.Unlock()

		cleanupStart := time.Now()
		if err := h.cleanupTemporaryFiles(); err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"duration_ms": time.Since(cleanupStart).Milliseconds(),
			}).Fatal("Failed to clean up temporary files during shutdown")
		}

		log.WithFields(log.Fields{
			"duration_ms": time.Since(cleanupStart).Milliseconds(),
		}).Info("Cleanup completed, exiting")

		os.Exit(0)
	}()
}

// finalize will move all files with a given suffix into place.
func (h *Harvest) finalize(suffix string) error {
	var renamed []string

	h.Lock()
	defer h.Unlock()

	tempFiles := h.temporaryFilesSuffix(suffix)

	log.WithFields(log.Fields{
		"count":  len(tempFiles),
		"suffix": suffix,
	}).Debug("Finalizing temporary files")

	for _, src := range tempFiles {
		dst := fmt.Sprintf("%s.gz", strings.Replace(src, suffix, "", -1))

		log.WithFields(log.Fields{
			"src": src,
			"dst": dst,
		}).Debug("Moving and compressing file")

		var err error
		startTime := time.Now()
		if err = MoveCompressFile(src, dst); err == nil {
			log.WithFields(log.Fields{
				"src":         src,
				"dst":         dst,
				"duration_ms": time.Since(startTime).Milliseconds(),
			}).Debug("File moved and compressed successfully")
			renamed = append(renamed, dst)
			continue
		}

		// Log the error
		log.WithFields(log.Fields{
			"src":   src,
			"dst":   dst,
			"error": err.Error(),
		}).Error("Failed to move and compress file")

		// Try to cleanup all the already renamed files.
		log.WithFields(log.Fields{
			"count": len(renamed),
		}).Warn("Attempting to clean up already renamed files due to error")

		for _, fn := range renamed {
			if e := os.Remove(fn); err != nil {
				if ee, ok := err.(*os.PathError); ok && ee.Err == syscall.ENOENT {
					log.WithFields(log.Fields{
						"filename": fn,
					}).Debug("File already removed")
					continue
				}

				log.WithFields(log.Fields{
					"filename": fn,
					"error":    e.Error(),
				}).Error("Failed to remove file during cleanup")

				return &MultiError{[]error{
					err,
					e,
					fmt.Errorf("inconsistent cache state; start over and purge %s", h.Dir())},
				}
			}

			log.WithFields(log.Fields{
				"filename": fn,
			}).Debug("Removed file during cleanup")
		}
		return err
	}

	if len(renamed) > 0 {
		log.WithFields(log.Fields{
			"count": len(renamed),
		}).Info("Moved files into place")
	} else {
		log.Debug("No files to finalize")
	}

	return nil
}

// defaultInterval returns a harvesting interval based on the cached
// state or earliest date, if this endpoint was not harvested before.
// If the harvest already has a From value set, we use it as earliest date.
func (h *Harvest) defaultInterval() (Interval, error) {
	var earliestDate time.Time
	var err error

	// refs #9100
	if h.Config.From == "" {
		earliestDate, err = h.earliestDate()
	} else {
		earliestDate, err = time.Parse("2006-01-02", h.Config.From)
	}
	if err != nil {
		return Interval{}, err
	}

	// Last value for this directory.
	laster := DirLaster{
		Dir:          h.Dir(),
		DefaultValue: earliestDate.Format("2006-01-02"),
		ExtractorFunc: func(fi os.FileInfo) string {
			groups := fnPattern.FindStringSubmatch(fi.Name())
			if len(groups) > 1 {
				return groups[1]
			}
			return ""
		},
	}

	last, err := laster.Last()
	if err != nil {
		return Interval{}, err
	}

	begin, err := time.Parse("2006-01-02", last)
	if err != nil {
		return Interval{}, err
	}

	if last != laster.DefaultValue {
		// Add a single day, only if we are not just starting.
		begin = begin.AddDate(0, 0, 1)
	}

	var end time.Time
	if h.Config.Until != "" {
		end, err = time.Parse("2006-01-02", h.Config.Until)
		if err != nil {
			log.WithFields(log.Fields{
				"until": h.Config.Until,
				"error": err.Error(),
			}).Error("Failed to parse custom end date")
			return Interval{}, err
		}
		log.WithFields(log.Fields{
			"end_date": end,
		}).Info("Using custom end date")
	} else {
		end = now.New(h.Started.AddDate(0, 0, -1)).EndOfDay()
		log.WithFields(log.Fields{
			"end_date": end,
		}).Debug("Using default end date (yesterday)")
	}

	if last == end.Format("2006-01-02") {
		return Interval{}, ErrAlreadySynced
	}
	return Interval{Begin: begin, End: end}, nil
}

// retry attempts an operation with exponential backoff
func (h *Harvest) retry(operation func() (*Response, error)) (*Response, error) {
	var lastErr error
	delay := h.Config.RetryDelay

	log.WithFields(log.Fields{
		"max_retries":   h.Config.MaxRetries,
		"initial_delay": delay,
		"backoff":       h.Config.RetryBackoff,
	}).Debug("Starting operation with retry mechanism")

	for attempt := 0; attempt <= h.Config.MaxRetries; attempt++ {
		if attempt > 0 {
			log.WithFields(log.Fields{
				"attempt":     attempt,
				"max_retries": h.Config.MaxRetries,
				"delay":       delay,
			}).Info("Retry attempt")

			time.Sleep(delay)
			// Apply backoff for next attempt
			delay = time.Duration(float64(delay) * h.Config.RetryBackoff)
		}

		startTime := time.Now()
		resp, err := operation()
		duration := time.Since(startTime)

		if err == nil {
			log.WithFields(log.Fields{
				"attempt":     attempt,
				"duration_ms": duration.Milliseconds(),
			}).Debug("Operation succeeded")
			return resp, nil
		}

		// Save the error for potential return
		lastErr = err

		// Log detailed error information
		logFields := log.Fields{
			"attempt":     attempt + 1,
			"max_retries": h.Config.MaxRetries,
			"error":       err.Error(),
			"duration_ms": duration.Milliseconds(),
		}

		// Add HTTP status code if available
		if httpErr, ok := err.(HTTPError); ok {
			logFields["status_code"] = httpErr.StatusCode
			logFields["url"] = httpErr.URL.String()
		}

		// Check if we should retry based on the error
		if !h.shouldRetry(err) {
			log.WithFields(logFields).Error("Request failed, not retrying")
			return nil, err
		}

		log.WithFields(logFields).Warn("Request failed, will retry")
	}

	log.WithFields(log.Fields{
		"max_retries": h.Config.MaxRetries,
		"error":       lastErr.Error(),
	}).Error("Operation failed after maximum retries")

	return nil, fmt.Errorf("failed after %d retries: %w", h.Config.MaxRetries, lastErr)
}

// shouldRetry determines if an error should trigger a retry
func (h *Harvest) shouldRetry(err error) bool {
	// Don't retry if we're not configured to handle HTTP errors
	if !h.Config.IgnoreHTTPErrors {
		return false
	}
	// Check for specific HTTP errors that we want to retry
	if httpErr, ok := err.(HTTPError); ok {
		switch httpErr.StatusCode {
		case 408, // Request Timeout
			429, // Too Many Requests
			500, // Internal Server Error
			502, // Bad Gateway
			503, // Service Unavailable
			504: // Gateway Timeout
			return true
		}
	}
	// Check for network-related errors
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "no such host") ||
		strings.Contains(err.Error(), "timeout") {
		return true
	}
	return false
}

// run runs a harvest: one request plus subsequent tokens.
func (h *Harvest) run() (err error) {
	log.WithFields(log.Fields{
		"baseURL": h.Config.BaseURL,
		"format":  h.Config.Format,
		"set":     h.Config.Set,
	}).Info("Starting harvest run")

	defer func() {
		cleanupStart := time.Now()
		if e := h.cleanupTemporaryFiles(); e != nil {
			log.WithFields(log.Fields{
				"error":       e.Error(),
				"duration_ms": time.Since(cleanupStart).Milliseconds(),
			}).Error("Failed to cleanup temporary files")

			if err != nil {
				err = &MultiError{[]error{err, e}}
			}
			err = e
		} else {
			log.WithFields(log.Fields{
				"duration_ms": time.Since(cleanupStart).Milliseconds(),
			}).Debug("Temporary files cleanup completed")
		}
	}()

	if h.Config.DisableSelectiveHarvesting {
		log.Info("Selective harvesting disabled, running without intervals")
		return h.runInterval(Interval{})
	}

	intervalStart := time.Now()
	interval, err := h.defaultInterval()
	if err != nil {
		log.WithFields(log.Fields{
			"error":       err.Error(),
			"duration_ms": time.Since(intervalStart).Milliseconds(),
		}).Error("Failed to get default interval")
		return fmt.Errorf("failed to get default interval: %w", err)
	}

	log.WithFields(log.Fields{
		"begin":       interval.Begin,
		"end":         interval.End,
		"duration_ms": time.Since(intervalStart).Milliseconds(),
	}).Info("Default interval determined")

	var intervals []Interval

	switch {
	case h.Config.HourlyInterval:
		intervals = interval.HourlyIntervals()
		log.WithFields(log.Fields{
			"count": len(intervals),
		}).Info("Using hourly intervals")
	case h.Config.DailyInterval:
		intervals = interval.DailyIntervals()
		log.WithFields(log.Fields{
			"count": len(intervals),
		}).Info("Using daily intervals")
	default:
		intervals = interval.MonthlyIntervals()
		log.WithFields(log.Fields{
			"count": len(intervals),
		}).Info("Using monthly intervals")
	}

	for i, iv := range intervals {
		log.WithFields(log.Fields{
			"interval_index":  i,
			"total_intervals": len(intervals),
			"begin":           iv.Begin,
			"end":             iv.End,
		}).Info("Processing interval")

		if err := h.runInterval(iv); err != nil {
			if h.Config.IgnoreUnexpectedEOF && err == io.ErrUnexpectedEOF {
				log.WithFields(log.Fields{
					"interval_index": i,
					"begin":          iv.Begin,
					"end":            iv.End,
				}).Warn("Ignoring unexpected EOF and moving to next interval")
				continue
			}

			log.WithFields(log.Fields{
				"interval_index": i,
				"begin":          iv.Begin,
				"end":            iv.End,
				"error":          err.Error(),
			}).Error("Failed to process interval")

			return err
		}

		log.WithFields(log.Fields{
			"interval_index": i,
			"begin":          iv.Begin,
			"end":            iv.End,
		}).Info("Interval processed successfully")
	}

	log.WithFields(log.Fields{
		"interval_count": len(intervals),
	}).Info("Harvest run completed successfully")

	return nil
}

// runInterval runs a selective harvest on the given interval.
func (h *Harvest) runInterval(iv Interval) error {
	suffix := fmt.Sprintf("-tmp-%d", rand.Intn(999999999))

	log.WithFields(log.Fields{
		"begin":  iv.Begin,
		"end":    iv.End,
		"suffix": suffix,
	}).Info("Starting interval harvest")

	var token string
	var i, empty int
	startTime := time.Now()

	for {
		if h.Config.MaxRequests == i {
			log.WithFields(log.Fields{
				"max_requests":  h.Config.MaxRequests,
				"requests_made": i,
			}).Warn("Max requests limit reached")
			break
		}

		req := Request{
			BaseURL:                 h.Config.BaseURL,
			MetadataPrefix:          h.Config.Format,
			Verb:                    "ListRecords",
			Set:                     h.Config.Set,
			ResumptionToken:         token,
			CleanBeforeDecode:       h.Config.CleanBeforeDecode,
			SuppressFormatParameter: h.Config.SuppressFormatParameter,
			ExtraHeaders:            h.Config.ExtraHeaders,
		}

		var filedate string
		if h.Config.DisableSelectiveHarvesting {
			// Used, when endpoint cannot handle from and until.
			filedate = h.Started.Format("2006-01-02")
		} else {
			filedate = iv.End.Format("2006-01-02")
			req.From = iv.Begin.Format(h.dateLayout())
			req.Until = iv.End.Format(h.dateLayout())
		}

		requestFields := log.Fields{
			"request_num":  i + 1,
			"filedate":     filedate,
			"token":        token,
			"from":         req.From,
			"until":        req.Until,
			"elapsed_time": time.Since(startTime),
		}
		log.WithFields(requestFields).Info("Preparing request")

		if h.Config.Delay > 0 {
			log.WithFields(log.Fields{
				"delay_ms": h.Config.Delay.Milliseconds(),
			}).Debug("Applying delay before request")
			time.Sleep(h.Config.Delay)
		}

		// Use retry mechanism for the request
		requestStart := time.Now()
		resp, err := h.retry(func() (*Response, error) {
			return h.Client.Do(&req)
		})
		requestDuration := time.Since(requestStart)

		requestFields["duration_ms"] = requestDuration.Milliseconds()

		if err != nil {
			requestFields["error"] = err.Error()

			// If we've exhausted all retries and still have an error
			if !h.Config.IgnoreHTTPErrors {
				log.WithFields(requestFields).Error("Failed to make request after retries")
				return fmt.Errorf("failed to make request after retries: %w", err)
			}

			// If we're ignoring HTTP errors, continue to next iteration
			log.WithFields(requestFields).Warn("Ignoring HTTP error and continuing")
			i++
			continue
		}

		// Handle OAI specific errors
		if resp.Error.Code != "" {
			requestFields["oai_error_code"] = resp.Error.Code
			requestFields["oai_error_message"] = resp.Error.Message

			// Rare case, where a resumptionToken is given, but it leads to
			// noRecordsMatch - we still want to save, whatever we got up until
			// this point, so we break here.
			switch resp.Error.Code {
			case "noRecordsMatch":
				if !resp.HasResumptionToken() {
					log.WithFields(requestFields).Info("No records match, ending harvest")
					break
				}
				log.WithFields(requestFields).Warn("ResumptionToken set and noRecordsMatch, continuing")
			case "badResumptionToken":
				log.WithFields(requestFields).Warn("BadResumptionToken, might signal end-of-harvest")
			case "InternalException":
				// #9717, InternalException Could not send Message.
				log.WithFields(requestFields).Warn("InternalException: retrying request in a few instants...")
				time.Sleep(30 * time.Second)
				i++ // Count towards the total request limit.
				continue
			default:
				log.WithFields(requestFields).Error("Unhandled OAI error")
				return resp.Error
			}
		}

		// The filename consists of the right boundary (until), the
		// serial number of the request and a suffix, marking this
		// request in progress.
		filename := filepath.Join(h.Dir(), fmt.Sprintf("%s-%08d.xml%s", filedate, i, suffix))
		requestFields["filename"] = filename

		if b, err := xml.Marshal(resp); err == nil {
			writeStart := time.Now()
			if e := ioutil.WriteFile(filename, b, 0644); e != nil {
				log.WithFields(log.Fields{
					"filename": filename,
					"error":    e.Error(),
				}).Error("Failed to write response to file")
				return e
			}

			log.WithFields(log.Fields{
				"filename":    filename,
				"size_bytes":  len(b),
				"duration_ms": time.Since(writeStart).Milliseconds(),
			}).Info("Wrote response to file")
		} else {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("Failed to marshal response to XML")
			return err
		}

		// Check for resumption token
		prev := token
		if token = resp.GetResumptionToken(); token == "" {
			log.WithFields(requestFields).Info("No resumption token, harvest complete")
			break
		}

		requestFields["new_token"] = token

		// Issue first observed at
		// https://gssrjournal.com/gssroai/?resumptionToken=33NjdYRs708&verb=ListRecords,
		// would spill the disk.
		if prev == token {
			url, _ := req.URL()
			log.WithFields(log.Fields{
				"token": token,
				"url":   url.String(),
			}).Warn("Token did not change, assume server issue, moving to next window")
			break
		}

		// Track record count and empty responses
		if len(resp.ListRecords.Records) > 0 {
			requestFields["record_count"] = len(resp.ListRecords.Records)
			log.WithFields(requestFields).Info("Response contains records")
			empty = 0
		} else {
			empty++
			log.WithFields(log.Fields{
				"empty_count":       empty,
				"max_empty_allowed": h.Config.MaxEmptyResponses,
			}).Warn("Successive empty response")
		}

		if empty == h.Config.MaxEmptyResponses {
			log.WithFields(log.Fields{
				"max_empty_responses": h.Config.MaxEmptyResponses,
			}).Warn("Max number of empty responses reached")
			break
		}

		i++
	}

	finalizeStart := time.Now()
	err := h.finalize(suffix)

	if err != nil {
		log.WithFields(log.Fields{
			"error":       err.Error(),
			"suffix":      suffix,
			"duration_ms": time.Since(finalizeStart).Milliseconds(),
		}).Error("Failed to finalize harvest")
		return err
	}

	log.WithFields(log.Fields{
		"requests":    i,
		"duration":    time.Since(startTime),
		"begin":       iv.Begin,
		"end":         iv.End,
		"duration_ms": time.Since(finalizeStart).Milliseconds(),
	}).Info("Interval harvest completed")

	return nil
}

// earliestDate returns the earliest date as a time.Time value.
func (h *Harvest) earliestDate() (time.Time, error) {
	// Different granularities are possible: https://eudml.org/oai/OAIHandler?verb=Identify
	// First occurence of a non-standard granularity: https://t3.digizeitschriften.de/oai2/

	log.WithFields(log.Fields{
		"granularity":        h.Identify.Granularity,
		"earliest_datestamp": h.Identify.EarliestDatestamp,
	}).Debug("Determining earliest date")

	switch strings.ToLower(h.Identify.Granularity) {
	case "yyyy-mm-dd":
		if len(h.Identify.EarliestDatestamp) <= 10 {
			date, err := time.Parse("2006-01-02", h.Identify.EarliestDatestamp)
			if err != nil {
				log.WithFields(log.Fields{
					"datestamp": h.Identify.EarliestDatestamp,
					"error":     err.Error(),
				}).Error("Failed to parse earliest date with YYYY-MM-DD format")
				return time.Time{}, err
			}
			log.WithFields(log.Fields{
				"date": date,
			}).Debug("Parsed earliest date with YYYY-MM-DD format")
			return date, nil
		}

		date, err := time.Parse("2006-01-02", h.Identify.EarliestDatestamp[:10])
		if err != nil {
			log.WithFields(log.Fields{
				"datestamp": h.Identify.EarliestDatestamp[:10],
				"error":     err.Error(),
			}).Error("Failed to parse earliest date with truncated YYYY-MM-DD format")
			return time.Time{}, err
		}
		log.WithFields(log.Fields{
			"date":            date,
			"original_length": len(h.Identify.EarliestDatestamp),
			"truncated_to":    10,
		}).Debug("Parsed earliest date with truncated YYYY-MM-DD format")
		return date, nil

	case "yyyy-mm-ddthh:mm:ssz":
		// refs. #8825
		if len(h.Identify.EarliestDatestamp) >= 10 && len(h.Identify.EarliestDatestamp) < 20 {
			date, err := time.Parse("2006-01-02", h.Identify.EarliestDatestamp[:10])
			if err != nil {
				log.WithFields(log.Fields{
					"datestamp": h.Identify.EarliestDatestamp[:10],
					"error":     err.Error(),
				}).Error("Failed to parse earliest date with truncated YYYY-MM-DDThh:mm:ssZ format")
				return time.Time{}, err
			}
			log.WithFields(log.Fields{
				"date":            date,
				"original_length": len(h.Identify.EarliestDatestamp),
				"truncated_to":    10,
			}).Debug("Parsed earliest date with truncated YYYY-MM-DDThh:mm:ssZ format")
			return date, nil
		}

		date, err := time.Parse("2006-01-02T15:04:05Z", h.Identify.EarliestDatestamp)
		if err != nil {
			log.WithFields(log.Fields{
				"datestamp": h.Identify.EarliestDatestamp,
				"error":     err.Error(),
			}).Error("Failed to parse earliest date with YYYY-MM-DDThh:mm:ssZ format")
			return time.Time{}, err
		}
		log.WithFields(log.Fields{
			"date": date,
		}).Debug("Parsed earliest date with YYYY-MM-DDThh:mm:ssZ format")
		return date, nil

	default:
		log.WithFields(log.Fields{
			"granularity": h.Identify.Granularity,
		}).Error("Invalid granularity format")
		return time.Time{}, ErrInvalidEarliestDate
	}
}

// identify runs an OAI identify request and caches the result.
func (h *Harvest) identify() error {
	log.WithFields(log.Fields{
		"baseURL": h.Config.BaseURL,
	}).Info("Identifying repository")

	req := Request{
		Verb:         "Identify",
		BaseURL:      h.Config.BaseURL,
		ExtraHeaders: h.Config.ExtraHeaders,
	}

	if h.Client == nil {
		h.Client = DefaultClient
		log.Debug("Using default client for identify request")
	}

	startTime := time.Now()
	resp, err := h.Client.Do(&req)
	duration := time.Since(startTime)

	if err != nil {
		log.WithFields(log.Fields{
			"error":       err.Error(),
			"baseURL":     h.Config.BaseURL,
			"duration_ms": duration.Milliseconds(),
		}).Warn("Identify request failed, trying workaround")

		// try to workaround for the whole harvest
		if h.Config.ExtraHeaders == nil {
			h.Config.ExtraHeaders = make(http.Header)
		}
		h.Config.ExtraHeaders.Set("Accept-Encoding", "identity")

		// also apply to this request
		req.ExtraHeaders = h.Config.ExtraHeaders

		log.WithFields(log.Fields{
			"headers": req.ExtraHeaders,
		}).Debug("Retrying identify with modified headers")

		retryStart := time.Now()
		resp, err = h.Client.Do(&req)
		retryDuration := time.Since(retryStart)

		if err != nil {
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"baseURL":     h.Config.BaseURL,
				"duration_ms": retryDuration.Milliseconds(),
			}).Error("Identify request failed after workaround")
			return err
		}

		log.WithFields(log.Fields{
			"duration_ms": retryDuration.Milliseconds(),
		}).Info("Identify request succeeded with workaround")
	} else {
		log.WithFields(log.Fields{
			"duration_ms": duration.Milliseconds(),
		}).Info("Identify request succeeded")
	}

	h.Identify = &resp.Identify

	log.WithFields(log.Fields{
		"repository_name":    resp.Identify.RepositoryName,
		"earliest_datestamp": resp.Identify.EarliestDatestamp,
		"granularity":        resp.Identify.Granularity,
		"protocol_version":   resp.Identify.ProtocolVersion,
		"deleted_record":     resp.Identify.DeletedRecord,
	}).Info("Repository identified")

	return nil
}

// init takes configuration from the environment, if there is any.
func init() {
	if dir := os.Getenv("METHA_DIR"); dir != "" {
		BaseDir = dir
	}

	// Setup logging with function and file information
	SetupLogging()
}
