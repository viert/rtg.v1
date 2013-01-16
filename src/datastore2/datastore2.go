/**
 * Created with IntelliJ IDEA.
 * User: viert
 * Date: 12/3/12
 * Time: 2:51 PM
 * To change this template use File | Settings | File Templates.
 */
package datastore2

import (
	"container/list"
	"sync"
	"fmt"
	"time"
)

type DataPair struct {
	Timestamp		int64
	Data			float64
}

type DataStore struct {
	Locker		sync.Mutex
	Store		map[string]*HostStore
}

type HostStore struct {
	Locker		sync.Mutex
	Store		map[string]*GraphStore
}

type GraphStore struct {
	Config		struct {
		Expire		int64 // Seconds
	}
	Locker		sync.Mutex
	Store		map[string]*FieldStore
}

type FieldStore struct {
	GraphStore	*GraphStore
	Store		*list.List
}

const INDENT 	string = "    "
const ARROW 	string = "--> "

func NewDataStore() *DataStore {
	ds := new(DataStore)
	ds.Store = make(map[string]*HostStore)
	go ds.expireFunc()
	return ds
}

func (ds *DataStore) expireFunc() {

	ticker := time.NewTicker(time.Duration(1)*time.Second)
	for	{
		<-ticker.C
		ds.Locker.Lock()
		for _, hoststore := range ds.Store {
			hoststore.Locker.Lock()
			for _, graphstore := range hoststore.Store {
				expTimestamp := time.Now().Unix() - graphstore.Config.Expire
				graphstore.Locker.Lock()
				for _, fieldstore := range graphstore.Store {
					toRemove := make([]*list.Element,0, 100)
					for item := fieldstore.Store.Front(); item != nil; item = item.Next() {
						dp, asserted := item.Value.(DataPair)
						if asserted && dp.Timestamp < expTimestamp {
							toRemove = append(toRemove, item)
						}
					}
					for _, item := range toRemove {
						fieldstore.Store.Remove(item)
					}
				}
				graphstore.Locker.Unlock()
			}
			hoststore.Locker.Unlock()
		}
		ds.Locker.Unlock()
	}

}

func (ds *DataStore) GetGraphDataMaxLength(hostname, graphname string) int64 {
	if !ds.hostExists(hostname) { return 0 }
	hs := ds.Store[hostname]
	if !hs.graphExists(graphname) { return 0 }
	gs := hs.Store[graphname]
	return gs.Config.Expire
}

func (ds *DataStore) hostExists(hostname string) bool {
	_, ok := ds.Store[hostname]
	return ok
}

func (ds *DataStore) newHostStore(hostname string) {
	hs := new(HostStore)
	hs.Store = make(map[string]*GraphStore)
	ds.Locker.Lock()
	ds.Store[hostname] = hs
	ds.Locker.Unlock()
}

func (hs *HostStore) graphExists(graphname string) bool {
	_, ok := hs.Store[graphname]
	return ok
}

func (hs *HostStore) newGraphStore(graphname string) {
	gs := new(GraphStore)
	gs.Config.Expire = 60 // 10 minutes

	gs.Store = make(map[string]*FieldStore)
	hs.Locker.Lock()
	hs.Store[graphname] = gs
	hs.Locker.Unlock()
}

func (gs *GraphStore) fieldExists(fieldname string) bool {
	_, ok := gs.Store[fieldname]
	return ok
}

func (gs *GraphStore) newFieldStore(fieldname string) {
	fs := new(FieldStore)
	fs.GraphStore = gs
	fs.Store = list.New()
	gs.Locker.Lock()
	gs.Store[fieldname] = fs
	gs.Locker.Unlock()
}

func (fs *FieldStore) pushData(data DataPair) bool {
	locker := fs.GraphStore.Locker

	locker.Lock()
	defer locker.Unlock()

	item := fs.Store.Back()
	if item == nil {
		fs.Store.PushBack(data)
		return true
	}

	for ; item != nil; item = item.Prev() {
		dp, asserted := item.Value.(DataPair)
		if asserted && dp.Timestamp < data.Timestamp {
			fs.Store.InsertAfter(data, item)
			return true
		}
	}

	return false
}

func (ds *DataStore) SetExpire(hostname, graphname string, expire int64) {
	if !ds.hostExists(hostname) { return }
	hs := ds.Store[hostname]
	if !hs.graphExists(graphname) { return }
	gs := hs.Store[graphname]
	if gs.Config.Expire == expire { return }
	gs.Config.Expire = expire
}

func (ds *DataStore) PushData(hostname, graphname, fieldname string, data float64, timestamp int64) bool {
	if !ds.hostExists(hostname) {
		ds.newHostStore(hostname)
	}

	hs := ds.Store[hostname]
	if !hs.graphExists(graphname) {
		hs.newGraphStore(graphname)
	}

	gs := hs.Store[graphname]
	if !gs.fieldExists(fieldname) {
		gs.newFieldStore(fieldname)
	}

	fs := gs.Store[fieldname]
	return fs.pushData(DataPair{timestamp, data})
}

func (ds *DataStore) GetFields(hostname, graphname string) []string {
	if !ds.hostExists(hostname) { return nil }
	hs := ds.Store[hostname]
	if !hs.graphExists(graphname) { return nil }
	gs := hs.Store[graphname]

	fields := make([]string, 0, 100)
	gs.Locker.Lock()
	defer gs.Locker.Unlock()

	for fieldname, _ := range gs.Store {
		fields = append(fields, fieldname)
	}
	return fields
}

func (ds *DataStore) GetLatestItems(hostname, graphname string, timestamp int64) map[string][]DataPair {
	if !ds.hostExists(hostname) { return nil }
	hs := ds.Store[hostname]
	if !hs.graphExists(graphname) { return nil }
	gs := hs.Store[graphname]

	result := make(map[string][]DataPair)
	gs.Locker.Lock()
	defer gs.Locker.Unlock()

	for fieldname, fieldstore := range gs.Store {
		result[fieldname] = make([]DataPair, 0, 1000)

		// Optimization: if timestamp is 0, just get all items. if not, search from back to front then copy.

		if timestamp == 0 {
			for item := fieldstore.Store.Front(); item != nil; item = item.Next() {
				dp, asserted := item.Value.(DataPair)
				if asserted { result[fieldname] = append(result[fieldname], dp)	}
			}
		} else {
			item := fieldstore.Store.Back()
			// searching backward if store is not empty
			if item != nil {
				for ;item != nil;item = item.Prev() {
					dp, asserted := item.Value.(DataPair)
					if asserted && dp.Timestamp < timestamp {
						item = item.Next()
						break
					}
				}
				// didnt found. starting from the begining
				if item == nil { item = fieldstore.Store.Front() }
				// and copying
				for ;item != nil;item = item.Next() {
					dp, asserted := item.Value.(DataPair)
					if asserted { result[fieldname] = append(result[fieldname], dp) }
				}

			}
		}
	}
	return result
}

func (ds *DataStore) String() string {
	var result string = ""
	for hostname, hoststore := range ds.Store {
		result += fmt.Sprintln(hostname)
		for graphname, graphstore := range hoststore.Store {
			result += ARROW + fmt.Sprintln(graphname, graphstore.Config )
			for fieldname, fieldstore := range graphstore.Store {
				result += INDENT + ARROW + fmt.Sprintln(fieldname)
				result += INDENT + INDENT + ARROW + fmt.Sprint(fieldstore.Store.Len()," elements: [")
				for item := fieldstore.Store.Front(); item != nil; item = item.Next() {
					dp, asserted := item.Value.(DataPair)
					if asserted {
						result += fmt.Sprint(dp.Timestamp,":",dp.Data," ")
					}
				}
				result += "]\n"
			}
		}
	}
	return result
}
