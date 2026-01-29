package metha

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sethgrid/pester"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html/charset"
)

const (
	// DefaultTimeout on requests.
	DefaultTimeout = 10 * time.Minute
	// DefaultMaxRetries is the default number of retries on a single request.
	DefaultMaxRetries = 8
)

var (
	// StdClient is the standard lib http client.
	StdClient = &Client{Doer: http.DefaultClient}
	// DefaultClient is the more resilient client, that will retry and timeout.
	DefaultClient = &Client{Doer: CreateDoer(DefaultTimeout, DefaultMaxRetries)}
	// DefaultUserAgent to identify crawler, some endpoints do not like the Go
	// default (https://golang.org/src/net/http/request.go#L462), e.g.
	// https://calhoun.nps.edu/oai/request.
	DefaultUserAgent = fmt.Sprintf("metha/%s", Version)
	// ControlCharReplacer helps to deal with broken XML: http://eprints.vu.edu.au/perl/oai2. Add more
	// weird things to be cleaned before XML parsing here. Another faulty:
	// http://digitalcommons.gardner-webb.edu/do/oai/?from=2016-02-29&metadataPr
	// efix=oai_dc&until=2016-03-31&verb=ListRecords. Replace control chars
	// outside XML char range.
	ControlCharReplacer = strings.NewReplacer(
		"\u0000", "", "\u0001", "", "\u0002", "", "\u0003", "", "\u0004", "",
		"\u0005", "", "\u0006", "", "\u0007", "", "\u0008", "", "\u0009", "",
		"\u000B", "", "\u000C", "", "\u000E", "", "\u000F", "", "\u0010", "",
		"\u0011", "", "\u0012", "", "\u0013", "", "\u0014", "", "\u0015", "",
		"\u0016", "", "\u0017", "", "\u0018", "", "\u0019", "", "\u001A", "",
		"\u001B", "", "\u001C", "", "\u001D", "", "\u001E", "", "\u001F", "",
		"\uFFFD", "", "\uFFFE", "",
	)
)

// HTTPError saves details of an HTTP error.
type HTTPError struct {
	URL          *url.URL
	StatusCode   int
	RequestError error
}

// Error prints the error message.
func (e HTTPError) Error() string {
	return fmt.Sprintf("failed with %s on %s: %v", http.StatusText(e.StatusCode), e.URL, e.RequestError)
}

// CreateDoer will return http request clients with specific timeout and retry
// properties.
func CreateDoer(timeout time.Duration, retries int) Doer {
	tr := http.DefaultTransport
	tr.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	client := http.DefaultClient
	client.Transport = tr
	client.Timeout = timeout
	if timeout == 0 && retries == 0 {
		return client
	}
	c := pester.New()
	c.EmbedHTTPClient(client)
	c.Timeout = timeout // does this propagate to client
	c.MaxRetries = retries
	c.Backoff = pester.ExponentialBackoff
	c.Transport = tr
	return c
}

// CreateClient creates a client with timeout and retry properties.
func CreateClient(timeout time.Duration, retries int) *Client {
	return &Client{Doer: CreateDoer(timeout, retries)}
}

// Doer is a minimal HTTP interface.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Client can execute requests.
type Client struct {
	Doer Doer
}

// Do is a shortcut for DefaultClient.Do.
func Do(r *Request) (*Response, error) {
	return DefaultClient.Do(r)
}

// maybeCompressed detects compressed content and decompresses it on the fly.
func maybeCompressed(r io.Reader) (io.ReadCloser, error) {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if gr, err := gzip.NewReader(bytes.NewReader(buf)); err == nil {
		log.Println("decompress-on-the-fly")
		return gr, nil
	}
	return ioutil.NopCloser(bytes.NewReader(buf)), nil
}

// Do executes a single OAIRequest. ResumptionToken handling must happen in the
// caller. Only Identify and GetRecord requests will return a complete response.
func (c *Client) Do(r *Request) (*Response, error) {
	link, err := r.URL()
	if err != nil {
		return nil, err
	}
	log.Println(link)

	// Log the request details
	log.WithFields(log.Fields{
		"verb":            r.Verb,
		"baseURL":         r.BaseURL,
		"metadataPrefix":  r.MetadataPrefix,
		"set":             r.Set,
		"from":            r.From,
		"until":           r.Until,
		"resumptionToken": r.ResumptionToken,
		"url":             link.String(),
	}).Info("OAI-PMH request")

	req, err := http.NewRequest("GET", link.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", DefaultUserAgent) // Maybe https://codereview.appspot.com/7532043.
	for name, values := range r.ExtraHeaders {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}

	// Log all request headers
	headerFields := log.Fields{}
	for name, values := range req.Header {
		headerFields[name] = strings.Join(values, ", ")
	}
	log.WithFields(headerFields).Debug("Request headers")

	startTime := time.Now()
	resp, err := c.Doer.Do(req)
	duration := time.Since(startTime)

	if err != nil {
		log.WithFields(log.Fields{
			"error":    err.Error(),
			"url":      link.String(),
			"duration": duration,
		}).Error("HTTP request failed")
		return nil, err
	}

	// Log response status and headers
	respHeaderFields := log.Fields{
		"status":      resp.Status,
		"statusCode":  resp.StatusCode,
		"duration_ms": duration.Milliseconds(),
		"url":         link.String(),
	}
	for name, values := range resp.Header {
		respHeaderFields["resp_"+name] = strings.Join(values, ", ")
	}
	log.WithFields(respHeaderFields).Info("OAI-PMH response received")

	if resp.StatusCode >= 400 {
		log.WithFields(log.Fields{
			"statusCode": resp.StatusCode,
			"url":        link.String(),
		}).Error("HTTP error response")
		return nil, HTTPError{URL: link, RequestError: err, StatusCode: resp.StatusCode}
	}
	defer resp.Body.Close()
	var reader = resp.Body
	// Detect compressed response.
	reader, err = maybeCompressed(reader)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if r.CleanBeforeDecode {
		// Remove some chars, that the XML decoder will complain about.
		b, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		reader = ioutil.NopCloser(strings.NewReader(ControlCharReplacer.Replace(string(b))))
	}
	// Drain response XML, iterate over various XML encoding declarations.
	// Limit the amount we can read, to 1GB, cf. https://github.com/miku/metha/issues/35
	respBody, err := ioutil.ReadAll(io.LimitReader(reader, 1<<30))
	if err != nil {
		return nil, err
	}

	// Log response body size
	log.WithFields(log.Fields{
		"size_bytes": len(respBody),
		"url":        link.String(),
	}).Debug("Response body size")
	// refs #21812, hack around misleading XML declarations; we only cover
	// declared "UTF-8", but actually ... A rare issue nonetheless; add more
	// cases here if necessary; observed in the wild in 05/2022 at
	// http://digi.ub.uni-heidelberg.de/cgi-bin/digioai.cgi?from=2021-07-01T00:00:00Z&metadataPrefix=oai_dc&until=2021-07-31T23:59:59Z&verb=ListRecords.
	//
	// TODO: https://github.com/miku/metha/issues/35

	// Log first 1000 bytes of response for debugging
	previewSize := 1000
	if len(respBody) < previewSize {
		previewSize = len(respBody)
	}
	log.WithFields(log.Fields{
		"preview": string(respBody[:previewSize]),
		"url":     link.String(),
	}).Debug("Response body preview")

	decls := [][]byte{
		[]byte(`<?xml version="1.0" encoding="UTF-8"?>`),
		[]byte(`<?xml version="1.0" encoding="ISO-8859-1"?>`),
		[]byte(`<?xml version="1.0" encoding="WINDOWS-1252"?>`),
		[]byte(`<?xml version="1.0" encoding="UTF-16"?>`),
		[]byte(`<?xml version="1.0" encoding="US-ASCII"?>`),
	}
	for i, decl := range decls {
		body := bytes.Replace(respBody, []byte(`<?xml version="1.0" encoding="UTF-8"?>`), decl, 1)
		dec := xml.NewDecoder(bytes.NewReader(body))
		dec.CharsetReader = charset.NewReaderLabel
		dec.Strict = false
		var response Response
		if err := dec.Decode(&response); err != nil {
			if !bytes.HasPrefix(body, []byte(`<?xml version="1.0"`)) {
				log.WithFields(log.Fields{
					"error": err.Error(),
					"url":   link.String(),
				}).Error("XML decode failed completely")
				return nil, err
			}
			log.WithFields(log.Fields{
				"encoding": string(decl),
				"error":    err.Error(),
				"url":      link.String(),
			}).Debug("XML decode failed with encoding")
			continue
		}

		if i > 0 {
			log.WithFields(log.Fields{
				"encoding": string(decl),
				"url":      link.String(),
			}).Info("XML decode succeeded with adjusted encoding")
		}

		// Log response details
		responseFields := log.Fields{
			"url": link.String(),
		}

		if response.Error.Code != "" {
			responseFields["oai_error_code"] = response.Error.Code
			responseFields["oai_error_message"] = response.Error.Message
			log.WithFields(responseFields).Warn("OAI-PMH error in response")
		}

		// Log resumption token if present
		if token := response.GetResumptionToken(); token != "" {
			responseFields["resumption_token"] = token
			if size := response.CompleteListSize(); size != "" {
				responseFields["complete_list_size"] = size
			}
			if cursor := response.Cursor(); cursor != "" {
				responseFields["cursor"] = cursor
			}
			log.WithFields(responseFields).Info("Response contains resumption token")
		}

		// Log record count
		if len(response.ListRecords.Records) > 0 {
			responseFields["record_count"] = len(response.ListRecords.Records)
			log.WithFields(responseFields).Info("Response contains records")
		} else if len(response.ListIdentifiers.Headers) > 0 {
			responseFields["identifier_count"] = len(response.ListIdentifiers.Headers)
			log.WithFields(responseFields).Info("Response contains identifiers")
		} else if len(response.ListSets.Set) > 0 {
			responseFields["set_count"] = len(response.ListSets.Set)
			log.WithFields(responseFields).Info("Response contains sets")
		} else if len(response.ListMetadataFormats.MetadataFormat) > 0 {
			responseFields["format_count"] = len(response.ListMetadataFormats.MetadataFormat)
			log.WithFields(responseFields).Info("Response contains metadata formats")
		} else if response.Identify.RepositoryName != "" {
			responseFields["repository_name"] = response.Identify.RepositoryName
			responseFields["earliest_datestamp"] = response.Identify.EarliestDatestamp
			responseFields["granularity"] = response.Identify.Granularity
			log.WithFields(responseFields).Info("Response contains repository information")
		} else if response.GetRecord.Record.Header.Identifier != "" {
			responseFields["identifier"] = response.GetRecord.Record.Header.Identifier
			log.WithFields(responseFields).Info("Response contains a single record")
		} else {
			log.WithFields(responseFields).Warn("Response contains no records or identifiers")
		}

		return &response, nil
	}

	log.WithFields(log.Fields{
		"url": link.String(),
	}).Error("Failed to parse response with any encoding")

	return nil, fmt.Errorf("failed to parse response")
}
