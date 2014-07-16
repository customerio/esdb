package esdb

// Returns a list of spaces in the esdb database
func (db *Db) InspectSpaces() ([]*Space) {

    spaces := []*Space{}
    db.Iterate(func(s *Space) bool {
        spaces = append(spaces, s)
        return true
    })

    return spaces
}
// Dumps out all the events in the database as a map of {Space: []Event}
// (to preserve the extra space information)
func (db *Db) DumpAll() map[*Space][]*Event {

    var events map[*Space][]*Event

    events = make(map[*Space][]*Event)

    db.Iterate(func(s *Space) bool {
      s.Iterate(func(g string) bool {
        s.Scan(g, func(event *Event) bool {
          events[s] = append(events[s], event)
          return true
        })
        return true
      })
      return true
    })

    return events
}
