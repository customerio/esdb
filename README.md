# Event Stream Database

Immutable storage for timestamped event streams. Inspired by [CDB](http://cr.yp.to/cdb.html) and [LevelDB](http://en.wikipedia.org/wiki/LevelDB)'s SSTable file format.

At [Customer.io](http://customer.io), we process billions of events every month, and have a need for maintaining all historic events in full resolution in a simple manner (not feasible to simply distill into timeseries data).

After investigating strategies for maintaining these events, we enjoyed the simple approach of archiving old events into structured flat files which provides dead simple backups, restores, and needs no running process to work.  After investigating current strategies and getting down and dirty with CDB and LevelDB, we choose to create ESDB as a stategy dedicated to querying event stream data.

*WARNING: version 0.0.00000001. I wrote this in one day as a proof of concept. We're planning on using it in production but there's a lot of testing, fine-tuning, and potential large changes to do before then.*

### Example

Let's assume you're tracking website activity.

You would like to store all pageviews, clicks, and purchases for every user of your site.  You'd like to quickly scan through the entire history for processing. However, you'd also like to occasionally just retrieve purchase events without the overhead of scanning through all the pageview and click data.

```
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/customerio/esdb"
)

type event struct {
	customerId string
	timestamp  int
	eventType  string
	data       map[string]string
}

func main() {
	events := []event{
		event{"1", 1403534919, "page", map[string]string{"url": "http://mysite.com/"}},
		event{"1", 1403534920, "click", map[string]string{"button_text": "Checkout"}},
		event{"1", 1403534921, "page", map[string]string{"url": "http://mysite.com/checkout"}},
		event{"1", 1403534923, "purchase", map[string]string{"total": "42.99"}},
		event{"1", 1403534923, "page", map[string]string{"url": "http://mysite.com/thankyou"}},
		event{"2", 1403534919, "page", map[string]string{"url": "http://mysite.com/"}},
		event{"2", 1403534920, "click", map[string]string{"button_text": "About"}},
		event{"2", 1403534921, "page", map[string]string{"url": "http://mysite.com/about"}},
		event{"3", 1403534919, "page", map[string]string{"url": "http://mysite.com/"}},
		event{"3", 1403534920, "click", map[string]string{"button_text": "About"}},
		event{"3", 1403534921, "page", map[string]string{"url": "http://mysite.com/about"}},
		event{"3", 1403534922, "click", map[string]string{"button_text": "Checkout"}},
		event{"3", 1403534923, "purchase", map[string]string{"total": "126.99"}},
		event{"3", 1403534923, "page", map[string]string{"url": "http://mysite.com/thankyou"}},
	}

	// In case we've already created the file.
	os.Remove("activity.esdb")

	writer, err := esdb.New("activity.esdb")
	if err != nil {
		panic(err)
	}

	for _, e := range events {
		data, _ := json.Marshal(e.data)

		writer.Add(
			[]byte(e.customerId), // block the event will be stored under.
			e.timestamp,          // all events will be stored sorted by this value.
			data,                 // value can be any binary data.
			"",                   // grouping. "" here means no grouping, store sequentially by timestamp.
			[]string{e.eventType}, // We'll define one secondary index on event type.
		)
	}

	err = writer.Write()
	if err != nil {
		panic(err)
	}

	db, err := esdb.Open("activity.esdb")
	if err != nil {
		panic(err)
	}

	// Stream through all customer 1's activity
	fmt.Println("activity for 1:")

	db.Find([]byte("1")).Scan("", func(event *esdb.Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Stream through all customer 2's activity
	fmt.Println("\nactivity for 2:")

	db.Find([]byte("2")).Scan("", func(event *esdb.Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Just retrieve customer 1's purchases
	fmt.Println("\npurchases for 1:")

	db.Find([]byte("1")).ScanIndex("purchase", func(event *esdb.Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Just retrieve customer 3's clicks ordered descending
	fmt.Println("\nclicks for 3:")

	db.Find([]byte("3")).RevScanIndex("click", func(event *esdb.Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})
}
```

### Goals/Benefits

1. Fast streaming of events ordered by timestamp.

   Events are stored on disk sequentially ordered by timestamp (or, an optional grouping and timestamp). This means scanning through a series of ordered events is extremely fast!

2. Secondary indexes for quickly scanning through just a subset of events.

   Scanning though a subset of events is preferable to scanning through the entire event stream, especially when the event stream contains many events which you aren't currently interested. ESDB allows definition of secondary indexes which allow quick retrieval of just events in the index.
   
   Secondary indexes are retrieved in timestamp order, and can be scanned forward and backwards.
   
3. Low overhead.

   Event overhead: as little 3 bytes + 17 bytes for each secondary index for each event stored. The 17 byte secondary index overhead is only applied for indexes which the particular event is apart.
   
   File overhead: offset information based on the number of blocks and groupings which are created is maintained at the end of the file. Generally, in a reasonably sized file, this should be negligible as event data and per event overhead should be the main driver of file size.

### Format 

`TODO :(`

### Benchmarks

`TODO :(`
