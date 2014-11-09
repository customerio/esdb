package cluster

type Timer interface {
	Time(func())
}

type NilTimer struct{}

func (NilTimer) Time(f func()) {
	f()
}
