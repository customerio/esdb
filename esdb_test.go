package esdb

import (
	"encoding/csv"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

func fetchSpaceIndex(db *Db, id []byte, index, value string) []string {
	found := make([]string, 0)

	space := db.Find(id)

	if space != nil {
		space.ScanIndex(index, value, func(event *Event) bool {
			found = append(found, string(event.Data))
			return true
		})
	}

	return found
}

var evs events

func createDb() *Db {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.esdb")

	w, err := New("tmp/test.esdb")
	if err != nil {
		println(err.Error())
	}
	populate(w)
	err = w.Write()
	if err != nil {
		println(err.Error())
	}

	db, err := Open("tmp/test.esdb")
	if err != nil {
		println(err.Error())
	}

	return db
}

func populate(w *Writer) {
	evs = events{
		newEvent([]byte("1"), 2),
		newEvent([]byte("2"), 3),
		newEvent([]byte("3"), 1),
		newEvent([]byte("4"), 3),
		newEvent([]byte("5"), 1),
		newEvent([]byte("6"), 2),
	}

	w.Add([]byte("a"), evs[0].Data, evs[0].Timestamp, "g", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("a"), evs[1].Data, evs[1].Timestamp, "h", map[string]string{"ts": "", "i": "i2"})
	w.Add([]byte("a"), evs[2].Data, evs[2].Timestamp, "i", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("b"), evs[3].Data, evs[3].Timestamp, "g", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("b"), evs[4].Data, evs[4].Timestamp, "h", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("b"), evs[5].Data, evs[5].Timestamp, "i", map[string]string{"ts": "", "i": "i1"})
}

func TestSpaceIndexes(t *testing.T) {
	db := createDb()

	var tests = []struct {
		id    string
		index string
		value string
		want  []string
	}{
		{"a", "ts", "", []string{"2", "1", "3"}},
		{"a", "i", "i1", []string{"1", "3"}},
		{"a", "i", "i2", []string{"2"}},
		{"b", "ts", "", []string{"4", "6", "5"}},
		{"b", "i", "i1", []string{"4", "6", "5"}},
		{"b", "i", "i2", []string{}},
		{"b", "i", "i3", []string{}},
		{"c", "ts", "", []string{}},
	}

	for i, test := range tests {
		found := fetchSpaceIndex(db, []byte(test.id), test.index, test.value)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}

func TestBigEvent(t *testing.T) {
	println("TEST")
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.esdb")

	w, err := New("tmp/test.esdb")
	if err != nil {
		println(err.Error())
	}

	max := 200

	evs = events{}

	for i := 0; i < max; i++ {
		evs = append(evs, newEvent([]byte(strconv.Itoa(i)), max-i))
	}

	evs[max/2].Data = []byte(`{\"customer_id\":\"oe9b0lc85ntlcwn8ia1mk1lzf653sa0m\",\"customer_local_id\":\"4374\",\"data\":{\"comment_post_title\":\"Can 90% Of People Do Worse Than Average?\",\"comment_post_user_name\":\"Josh Silverman\",\"comment_url\":\"https://brilliant.org/community-problem/can-90-of-people-do-worse-than-average/?group=RExhWAyBt1sm#!/solution-comments/35027/35073\",\"comment_user_first_name\":\"josh\",\"comment_user_last_name\":\"silverman\",\"comment_user_name\":\"Josh Silverman\",\"comment_user_profile_url\":\"https://brilliant.org/profile/josh-8t4vhp/feed/\",\"notify_reason\":\"post\",\"notify_reason_text\":\"In the first case, Dan is looking at the number of followers that the user has, \\(f^i\\). \n\nThat number should of course have the average value \n\n\\[\\langle f \\rangle = \\frac1N\\sum\\limits_i f^i\\]\n\nIn the second case, Dan is looking at the average number of followers that user \\(i\\)'s followers have, \\(f_f^i\\). \n\nThis number is given by \n\n\\[ f_f^i = \\frac{1}{f_i} \\sum\\limits_{j \\in F_i} f_j \\]\n\nwhere \\(F_i\\) is the set of users who follow user \\(i\\). In other words, we are taking the average number of followers for users who follow user \\(i\\).\n\nThough it may seem that the number in the second case should be the same (on average) as the number in the first case, it can in fact be quite different. \n\nConsider the case of a friendship network that has the symmetric relation of \"friendship\", i.e., if \\(x\\) is a friend of \\(y\\), then \\(y\\) is a friend of \\(x\\).  Suppose there are people in the network who have no friends. \n\nWhen the average number of friends per user is calculated, the people with no friends will be included in the average. \n\nWhen the average number of friends that a user's friends have is included (friend's friends), such people are explicitly left out of the calculation. \n\nIn other words, the average number friend's friends is fundamentally biased in favor of people who have friends. \n\nThe general principle is that this calculation overemphasizes the contributions from people who have more friends. This is known as the \"friendship paradox\".\n\nOne could ask how, if in any way, does the relation (friend or follower) contribute to this property? This can in general be a tough nut to crack, but we can explore the consequences of two simple network types: \n\n- **1** the Twitter (and Brilliant) network where \"following\" is asymmetric (\\(x\\) can follow \\(y\\) even when \\(y\\) does not follow \\(x\\))\n- **2** the Facebook network, where friendship is symmetric.\n\n\\(\\large\\mbox{Twitter relation}\\)\n\nOn Twitter (and Brilliant), users are connected by directed edges: if an edge is directed from \\(x\\) to \\(y\\), then \\(x\\) follows \\(y\\) or \\(x\\rightarrow y\\). For \\(y\\rightarrow x\\), we need a separate edge going in the opposite direction.  Therefore, for everyone to follow everyone else, we require \\(n\\left(n-1\\right)\\) edges, compared to the \\(\\frac{n\\left(n-1\\right)}{2}\\) for Facebook.\n\n Consider a random network, generated according to the following prescription:\n\n    - Select a number of users and a number of following events.\n    - For each following event you wish to generate, randomly select two users (uniformly and without replacement) from the set of users. \n    - Create a directed edge from the second user to the first user, i.e. the second user follows the first.\n    - Repeat until you have the desired number of following events.\n\nThis kind of network can be easily implemented in the programming language of your choice.  Before we look at the results of simulating such a network, let's think about the \"follower\" relationship between users in a random network (RN). In a RN, the act of gaining a follower is divorced from the act of following someone. A user is equally like to follow 10 people and be followed by nobody as they are to be followed by 10 people and follow nobody. We do not expect much bias in the calculation of follower's followers compared to a given user's followers (no friendship paradox).\n\nBelow are the results of 10,000 rounds of simulation of a random Brilliant style network. The network contains 30 users with 435 following events (1/2 of all possible following events).\n\n- **Mean number of followers** \\(= \\frac{435}{30} = 14.5\\) (this must be the case)\n- **Mean number of follower's followers** = \\(14.497 \\pm 0.0364\\)\n- **Percent of users with fewer followers than their follower's followers** = \\(49.6 \\pm 0.06\\)\n\nIndeed, in a random follower network, there does not seem to be a sampling bias in calculating the average number of follower's followers. Half the users are less popular than their friends.\n\n\\(\\large\\mbox{Facebook relation}\\)\n\nOn Facebook, users are connected by undirected edges: if \\(x\\) is friends with \\(y\\), then \\(y\\) is friends with \\(x\\). \n\nAs with Brilliant, we can simulate such a network with a simple prescription:\n\n - Select a number of users and a number of friendship events.\n    - For each friendship you wish to generate, randomly select two users (uniformly and without replacement) from the set of users. \n    - Create an edge from between the two users.\n    - Repeat until you have the desired number of friendship.\n\nIn this case, even in a random network, we expect to see a difference between a user's friends, and friend's friends. As we said before, friend's friends is a weighted average that disproportionately favors the contributions from popular users. \n\nBelow are the results of 10,000 rounds of simulation of a random Facebook style network. The network contains 30 users and 218 friendships ($\\approx$ of all possible friendships).\n\n- **Mean number of friends** = \\(2\\times218/30\\) = 14.5333 (this must be the case)\n- **Mean number of follower's followers** = \\(15.0297 \\pm 0.001\\)\n- **Percent of users with fewer followers than their follower's followers** = \\(56.87 \\pm 0.05\\)\n\nWe can see clearly that even in a completely random network, the structure of the friendship relation is enough to produce a measurable boost and manifest the friendship paradox. Nearly 57% of users are less popular than their friends, solely due to the structure of the friendship relation.\n\nWe can see a negative correlation between the number of friends that a user has, and the number of friends their friends have:\n\n![](http://i62.tinypic.com/35mp8pw.png)\n\nHere we show a smoothed histogram showing the likelihood for a user in either style network to experience the friendship paradox.\n\n![](http://i62.tinypic.com/2hzslsh.png)\n\n\\(\\large\\mbox{Analysis}\\)\n\nThe friendship paradox is a counterintuitive result. Let's see if we can shed any light on it. \n\nAs we said earlier, the typical user has \\(\\langle f \\rangle = \\frac1N\\sum\\limits_i f^i = \\langle k \\rangle\\) friends/followers.\n\nTo calculate friend's friends, we have to take into account the number of connections a user has, as well as the kinds of user they are more or less likely to connect to.\n\nBy type of user, we mean, \"how many friends do they have?\". Are they they kind of user with two friendships?, three?, four?,  etc. Let the number of users with \\(k\\) friends be \\(N(k)\\). \n\nIf we pick a random friendship in a random Facebook style network, the probability that one of the friends is a user with \\(k\\) friends is equal to  to \n\n\\[\\begin{align} \n\\frac{kN(k)}{\\sum\\limits_k kN(k)} &= \\frac{kp(k)}{\\sum\\limits_k kp(k)} \\\\\n&= \\frac{kp(k)}{\\langle k \\rangle}\n\\end{align}\\].\n\nHere we have used \\(p(k) = N(k)/N_{tot}\\), the probability for a user to have \\(k\\) friends. Note, this doesn't hold in the Brilliant style random network.\n\nSo, we have the probability that any given friend of a user will have \\(k\\) friends, given by \\(\\frac{kp(k)}{\\langle k \\rangle}\\). We can now calculate the average number of friends that a user's friends have by averaging over all kinds of friends:\n\n\\[\\begin{align}\n\\langle f_f^i \\rangle &= \\sum\\limits_k k\\frac{kp(k)}{\\langle k \\rangle} \\\\\n&= \\sum\\limits_k \\frac{k^2p(k)}{\\langle k \\rangle} \\\\\n&= \\frac{\\langle k^2 \\rangle}{\\langle k \\rangle}\n\\end{align}\n\\]\n\nTherefore, \\(\\langle f_f^i \\rangle = \\frac{\\langle k^2 \\rangle}{\\langle k \\rangle}\\). We can make things more clear by rewriting this in terms of the variance and the mean.  The variance of a list of numbers \\(s_i\\) is given by \\(\\sigma^2(s) = \\langle s^2 \\rangle - \\langle s \\rangle ^2\\).\n\nTherefore, the average number of friend's friends in a random network is given by  \n\n\\[\\langle f_f^i \\rangle  = \\langle k \\rangle + \\frac{\\sigma^2(k)}{\\langle k \\rangle}\\]\n\nAs the variance of a dataset is strictly positive, this shows that the average number of friend's friend's will always be higher than the average number of friends, as long as the variance is not zero, i.e. as long as every user doesn't have the same number of friends. \n\nWe can try this out on the Facebook network above. The average variance in the number of friends per user was 6.99 while the mean number of friends per user was 14.53. Based on this, we'd predict the average number of friend's friends to be \\(14.53 + 6.99/14.53 \\approx 15.01\\) which is very close to the measured value of 15.02. \n\n\\(\\large\\mbox{Discussion}\\)\n\nAll of this seems to contradict the observation that Dan had about the Brilliant network, and it would if the Brilliant network were simply random, but it isn't. \n\nThe real Brilliant network likely reflects elements of the Twitter and Facebook style random networks. I.e. if user \\(x\\) followers user \\(y\\) there is a chance between zero (random Twitter network style) and one (Facebook style) for the reverse to happen. \n\nThe random networks we considered have a binomial distribution in the number of followers per user, which has relatively low variance. The real Brilliant network has a heavy tailed distribution with **much** more variance, i.e. a relatively few users have a large number of followers and and most have a few followers. \n\nMoreover, users don't select who to follow at random. In addition to following their friends, Brilliant users tend to follow popular users. Simple reasons for this might be that the popular user often shares interesting problems, sets, and notes, or that they post useful solutions to problems. Because there is a non-uniform distribution of popular content generation across the user base, we expect content generation to have a non-uniform effect on following patterns, which boosts the magnitude of the friendship paradox as we showed above. If an extremely popular users refollows a small number of their followers, we further expect that to bias the count of follower's followers.\n\nIn summary, the cause for Dan's observation is that users follow (sample) the base of other users non-uniformly.\",\"notify_reason_title\":\"Can 90% Of People Do Worse Than Average?\",\"notify_reason_unsubscribe_helper\":[\"https://brilliant.org/community-problem/can-90-of-people-do-worse-than-average/?group=RExhWAyBt1sm&unsubscribe_from_post=true#!/solution-comments/35027/\"],\"notify_reason_url\":\"https://brilliant.org/community-problem/can-90-of-people-do-worse-than-average/?group=RExhWAyBt1sm#!/solution-comments/35027/\",\"subscription_user_email\":\"jomarie_13_rasengan@yahoo.com.ph\",\"subscription_user_first_name\":\"Jomarie\",\"subscription_user_last_name\":\"Cabuello\",\"subscription_user_name\":\"Jomarie Cabuello\"},\"id\":\"9c450700-9974-0131-a9b5-7f66c6d96715\",\"name\":\"solutiondiscussions_new_comment_on_subscription\",\"session_id\":\"customer_oe9b0lc85ntlcwn8ia1mk1lzf653sa0m_events\",\"timestamp\":1396099997,\"type\":\"event\"}\n`)

	for i := 0; i < max; i++ {
		w.Add([]byte("a"), evs[i].Data, evs[i].Timestamp, "", map[string]string{})
	}

	err = w.Write()
	if err != nil {
		println(err.Error())
	}

	db, err := Open("tmp/test.esdb")
	if err != nil {
		println(err.Error())
	}

	i := 0

	db.Find([]byte("a")).Scan("", func(e *Event) bool {
		if string(e.Data) != string(evs[i].Data) {
			println("ffffffail", len(e.Data), len(evs[i].Data))
			t.Errorf("Case %d: Wrong event data: want: %s found: %s", i, string(evs[i].Data), string(e.Data))
		}
		i += 1
		return true
	})
}

func fetch100RowColumns(file string, index int) []string {
	f, _ := os.Open("testdata/million_visits.csv")
	c := csv.NewReader(f)

	columns := make([]string, 100)

	for i := 0; i < 100; i++ {
		row, err := c.Read()
		if err != nil {
			panic(err)
		}

		// third column is city, which
		// is an index in our test file.
		columns[i] = row[index]
	}

	return columns
}

func BenchmarkMillionEventDbScanSingle(b *testing.B) {
	// csv index 1 is host, which is the grouping in our test file.
	hosts := fetch100RowColumns("testdata/million_visits.csv", 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		host := hosts[rand.Intn(100)]
		db.Find([]byte("visit")).Scan(host, func(e *Event) bool {
			return false
		})
	}
}

func BenchmarkMillionEventDbScanIndexSingle(b *testing.B) {
	// csv index 2 is city, which is an index in our test file.
	cities := fetch100RowColumns("testdata/million_visits.csv", 2)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		city := cities[rand.Intn(100)]
		db.Find([]byte("visit")).ScanIndex("city", city, func(e *Event) bool {
			return false
		})
	}
}

func BenchmarkMillionEventDbScan500(b *testing.B) {
	// csv index 1 is host, which is the grouping in our test file.
	hosts := fetch100RowColumns("testdata/million_visits.csv", 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		count := 0

		host := hosts[rand.Intn(100)]
		db.Find([]byte("visit")).Scan(host, func(e *Event) bool {
			count += 1
			return count < 500
		})
	}
}

func BenchmarkMillionEventDbScanIndex500(b *testing.B) {
	// csv index 2 is city, which is an index in our test file.
	cities := fetch100RowColumns("testdata/million_visits.csv", 2)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		count := 0

		city := cities[rand.Intn(100)]
		db.Find([]byte("visit")).ScanIndex("city", city, func(e *Event) bool {
			count += 1
			return count < 500
		})
	}
}
