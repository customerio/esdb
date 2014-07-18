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
func (db *Db) DumpAll() map[string][]*Event {

    var events map[string][]*Event

    events = make(map[string][]*Event)

    db.Iterate(func(s *Space) bool {
      s.Iterate(func(g string) bool {
        s.Scan(g, func(event *Event) bool {
          events[string(s.Id)] = append(events[string(s.Id)], event)
          return true
        })
        return true
      })
      return true
    })

    return events
}
