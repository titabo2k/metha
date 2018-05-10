package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/miku/metha"
	log "github.com/sirupsen/logrus"
)

// Dc was generated 2018-05-10 14:57:24 by tir on sol.
type Dc struct {
	XMLName        xml.Name `xml:"dc"`
	Text           string   `xml:",chardata"`
	OaiDc          string   `xml:"oai_dc,attr"`
	Dc             string   `xml:"dc,attr"`
	Xsi            string   `xml:"xsi,attr"`
	SchemaLocation string   `xml:"schemaLocation,attr"`
	Title          []struct {
		Text string `xml:",chardata"` // The contribution of thesa...
		Lang string `xml:"lang,attr"`
	} `xml:"title"`
	Creator []struct {
		Text string `xml:",chardata"` // Casari Boccato, Vera Regi...
	} `xml:"creator"`
	Description []struct {
		Text string `xml:",chardata"` // From the interdisciplinar...
		Lang string `xml:"lang,attr"`
	} `xml:"description"`
	Publisher []struct {
		Text string `xml:",chardata"` // Ibersid: journal of infor...
		Lang string `xml:"lang,attr"`
	} `xml:"publisher"`
	Date struct {
		Text string `xml:",chardata"` // 2008-09-15
	} `xml:"date"`
	Type []struct {
		Text string `xml:",chardata"` // info:eu-repo/semantics/ar...
		Lang string `xml:"lang,attr"`
	} `xml:"type"`
	Format struct {
		Text string `xml:",chardata"` // application/pdf
	} `xml:"format"`
	Identifier struct {
		Text string `xml:",chardata"` // https://ibersid.eu/ojs/in...
	} `xml:"identifier"`
	Source []struct {
		Text string `xml:",chardata"` // Ibersid: journal of infor...
		Lang string `xml:"lang,attr"`
	} `xml:"source"`
	Language struct {
		Text string `xml:",chardata"` // spa
	} `xml:"language"`
	Relation struct {
		Text string `xml:",chardata"` // https://ibersid.eu/ojs/in...
	} `xml:"relation"`
	Rights []struct {
		Text string `xml:",chardata"` // © 2007-present Francisco...
		Lang string `xml:"lang,attr"`
	} `xml:"rights"`
}

type Result struct {
	Fortune string
	Err     error
}

type Search func(ctx context.Context) Result

func First(ctx context.Context, endpoints ...Search) Result {
	c := make(chan Result, len(endpoints))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	search := func(endpoint Search) { c <- endpoint(ctx) }
	for _, ep := range endpoints {
		go search(ep)
	}
	for {
		log.Println("select")
		select {
		case <-ctx.Done():
			log.Printf("context done")
			return Result{Err: ctx.Err()}
		case r := <-c:
			if r.Err == nil && len(r.Fortune) > 0 {
				return r
			}
			log.Printf("backend returned with an error or an empty description: %v", r.Err)
		}
	}
}

// createSearcher assembles a search type.
func createSearcher(endpoint string) Search {
	f := func(ctx context.Context) Result {
		client := metha.CreateClient(8*time.Second, 3)
		req := metha.Request{
			BaseURL:        endpoint,
			Verb:           "ListIdentifiers",
			MetadataPrefix: "oai_dc",
		}
		resp, err := client.Do(&req)
		if err != nil {
			return Result{Err: err}
		}
		var ids []string
		for _, h := range resp.ListIdentifiers.Headers {
			ids = append(ids, h.Identifier)
		}
		if len(ids) == 0 {
			return Result{Err: err}
		}
		rid := ids[rand.Intn(len(ids))]

		// Fetch a random record.
		req = metha.Request{
			BaseURL:        endpoint,
			Verb:           "GetRecord",
			MetadataPrefix: "oai_dc",
			Identifier:     rid,
		}
		resp, err = client.Do(&req)
		if err != nil {
			return Result{Err: err}
		}
		var record Dc
		dec := xml.NewDecoder(bytes.NewReader(resp.GetRecord.Record.Metadata.Body))
		dec.Strict = false
		if err := dec.Decode(&record); err != nil {
			return Result{Err: err}
		}
		if len(record.Description) > 0 {
			if len(record.Description[0].Text) == 0 {
				return Result{Err: fmt.Errorf("empty description")}
			}
			var buf bytes.Buffer
			io.WriteString(&buf, record.Description[0].Text)
			fmt.Fprintf(&buf, "\n\n    -- %s", endpoint)
			return Result{Fortune: buf.String(), Err: nil}
		}
		return Result{Err: fmt.Errorf("could not fetch fortune from %s", endpoint)}
	}
	return f
}

func main() {
	debug := flag.Bool("d", false, "debug output")
	k := flag.Int("k", 16, "number of endpoints to query in parallel")

	flag.Parse()

	if !*debug {
		log.SetOutput(ioutil.Discard)
	}

	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var searchers []Search
	for i := 0; i < *k; i++ {
		searchers = append(searchers, createSearcher(metha.RandomEndpoint()))
	}

	s := spinner.New(spinner.CharSets[25], 100*time.Millisecond)
	if !*debug {
		s.Writer = os.Stderr
		s.Start()
	}

	result := First(ctx, searchers...)

	if !*debug {
		s.Stop()
	}

	if result.Err != nil || result.Fortune == "" {
		fmt.Printf("No fortune available at this time.\n")
		if *debug {
			log.Printf("%v", result.Err)
		}
		os.Exit(1)
	}
	fmt.Println(result.Fortune)
}
