package stream

func Merge(destination string, streams []string) error {
	m, err := New(destination)
	if err != nil {
		return err
	}

	for _, path := range streams {
		s, err := Open(path)
		if err != nil {
			return err
		}

		_, err = s.Iterate(0, func(e *Event) bool {
			m.Write(e.Data, e.Indexes())
			return true
		})

		if err != nil {
			return err
		}
	}

	return m.Close()
}
