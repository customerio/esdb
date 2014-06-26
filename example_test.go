package esdb

import (
	"encoding/json"
	"fmt"
	"os"
)

type event struct {
	customerId string
	timestamp  int
	eventType  string
	data       map[string]string
}

func ExampleDb() {
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

	os.MkdirAll("tmp", 0755)

	// In case we've already created the file.
	os.Remove("tmp/activity.esdb")

	writer, err := New("tmp/activity.esdb")
	if err != nil {
		panic(err)
	}

	for _, e := range events {
		value, _ := json.Marshal(e.data)

		writer.Add(
			[]byte(e.customerId), // space the event will be stored under.
			value,                // value can be any binary data.
			e.timestamp,          // all events will be stored sorted by this value.
			"",                   // grouping. "" here means no grouping, store sequentially by timestamp.
			map[string]string{
				"type": e.eventType, // We'll define one secondary index on event type.
			},
		)
	}

	err = writer.Write()
	if err != nil {
		panic(err)
	}

	db, err := Open("tmp/activity.esdb")
	if err != nil {
		panic(err)
	}

	// Stream through all customer 1's activity
	fmt.Println("activity for 1:")
	db.Find([]byte("1")).Scan("", func(event *Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Stream through all customer 2's activity
	fmt.Println("\nactivity for 2:")
	db.Find([]byte("2")).Scan("", func(event *Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Just retrieve customer 1's purchases
	fmt.Println("\npurchases for 1:")
	db.Find([]byte("1")).ScanIndex("type", "purchase", func(event *Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Just retrieve customer 3's clicks ordered descending
	fmt.Println("\nclicks for 3:")
	db.Find([]byte("3")).RevScanIndex("type", "click", func(event *Event) bool {
		fmt.Println(string(event.Data))
		return true // continue
	})

	// Output:
	// activity for 1:
	// {"total":"42.99"}
	// {"url":"http://mysite.com/thankyou"}
	// {"url":"http://mysite.com/checkout"}
	// {"button_text":"Checkout"}
	// {"url":"http://mysite.com/"}
	//
	// activity for 2:
	// {"url":"http://mysite.com/about"}
	// {"button_text":"About"}
	// {"url":"http://mysite.com/"}
	//
	// purchases for 1:
	// {"total":"42.99"}
	//
	// clicks for 3:
	// {"button_text":"Checkout"}
	// {"button_text":"About"}
}
