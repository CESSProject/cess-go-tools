package cacher

import (
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/CESSProject/cess-go-tools/utils"
	"github.com/muesli/cache2go"
	"github.com/pkg/errors"
)

const (
	DEFAULT_EXPIRATION      = 60 * time.Minute
	DEFAULT_MAX_CACHE_SPACE = 512 * 1024 * 1024 * 1024
	DEFAULT_CACHE_NAME      = "file_cache"
)

type FileCache interface {
	MoveFileToCache(fname, fpath string) error
	SaveDataToCache(fname string, data []byte) error
	AddCacheRecord(fname, fpath string) error
	GetCacheRecord(fname string) (string, error)
	RemoveCacheRecord(fname string) error
	AddEmptyCacheRecord(fname string)
	GetCacheItem(fname string) (CacheItem, error)
	TraverseCache(f ForEachItems)
	FlushAndCleanCache(wantSize int64) bool
	AddCallbackOfAddItem(f ForItem)
	AddCallbackOfDeleteItem(f ForItem)
	GetLoadRatio() float64
}

type ForEachItems func(key interface{}, item CacheItem)
type ForItem func(item CacheItem)

type CacheItem interface {
	Data() interface{}
	Key() interface{}
	AccessCount() int64
	CreatedOn() time.Time
	AccessedOn() time.Time
	LifeSpan() time.Duration
}

type Cacher struct {
	lock       *sync.RWMutex
	cacher     *cache2go.CacheTable
	cacheSpace int64
	usedSpace  int64
	exp        time.Duration
	CacheDir   string
}

type CacheRecord struct {
	Cpath string
	Csize int64
}

func NewCacher(exp time.Duration, maxSpace int64, cacheDir string) FileCache {
	if exp <= 0 {
		exp = DEFAULT_EXPIRATION
	}
	if maxSpace <= 0 {
		maxSpace = DEFAULT_MAX_CACHE_SPACE
	}
	cacher := &Cacher{
		exp:        exp,
		lock:       &sync.RWMutex{},
		cacher:     cache2go.Cache(DEFAULT_CACHE_NAME),
		CacheDir:   cacheDir,
		cacheSpace: maxSpace,
	}
	cacher.cacher.SetAboutToDeleteItemCallback(func(ci *cache2go.CacheItem) {
		item, ok := ci.Data().(CacheRecord)
		if !ok {
			return
		}
		log.Println("remove file", item.Cpath, item.Csize)
		log.Println("is error", cacher.removeFile(item.Cpath))
	})
	return cacher
}

func (c *Cacher) AddCallbackOfAddItem(f ForItem) {
	c.cacher.AddAddedItemCallback(
		func(ci *cache2go.CacheItem) { f(ci) },
	)
}

func (c *Cacher) AddCallbackOfDeleteItem(f ForItem) {
	c.cacher.AddAboutToDeleteItemCallback(
		func(ci *cache2go.CacheItem) { f(ci) },
	)
}

func (c *Cacher) GetLoadRatio() float64 {
	used, total := 0.0, 0.0
	c.lock.RLock()
	used = float64(c.usedSpace)
	total = float64(c.cacheSpace)
	c.lock.RUnlock()
	if total <= 0 {
		return 0
	}
	return used / total
}

func (c *Cacher) MoveFileToCache(fname, fpath string) error {
	f, err := os.Stat(fpath)
	if err != nil {
		return errors.Wrap(err, "move file to cache error")
	}
	if f.IsDir() {
		return errors.Wrap(errors.New("not a file"), "move file to cache error")
	}
	cpath := path.Join(c.CacheDir, fname)
	f2, err := os.Stat(cpath)
	size := f.Size()
	if err == nil {
		if f2.Size() == size {
			c.cacher.Add(fname, c.exp, CacheRecord{Cpath: cpath, Csize: f2.Size()})
			return nil
		}
		size -= f2.Size()
	}

	dir := filepath.Dir(cpath)
	if _, err := os.Stat(dir); err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return errors.Wrap(err, "move file to cache error")
		}
	}

	input, err := os.Open(fpath)
	if err != nil {
		return errors.Wrap(err, "move file to cache error")
	}
	defer input.Close()
	output, err := os.Create(cpath)
	if err != nil {
		return errors.Wrap(err, "move file to cache error")
	}
	defer output.Close()

	c.lock.Lock()
	defer c.lock.Unlock()
	//add record and reomve expired records
	c.cacher.Add(fname, c.exp, CacheRecord{Cpath: cpath, Csize: size})

	free, err := utils.GetDirFreeSpace(dir)
	if err != nil {
		return errors.Wrap(err, "move file to cache error")
	}
	if c.usedSpace+size > c.cacheSpace || int64(free) < size {
		if !c.cacheSwapout(size) {
			c.cacher.Delete(fname)
			return errors.Wrap(errors.New("not enough cache space"), "move file to cache error")
		}
	}

	_, err = io.Copy(output, input)
	if err != nil {
		c.cacher.Delete(fname)
		return errors.Wrap(err, "move file to cache error")
	}
	c.usedSpace += size
	err = os.Remove(fpath)
	if err != nil {
		return errors.Wrap(err, "move file to cache error")
	}
	return nil
}

func (c *Cacher) SaveDataToCache(fname string, data []byte) error {
	cpath := path.Join(c.CacheDir, fname)
	size := int64(len(data))
	f, err := os.Stat(cpath)
	if err == nil {
		size = f.Size()
		if size == int64(len(data)) {
			c.cacher.Add(fname, c.exp, CacheRecord{Cpath: cpath, Csize: size})
			return nil
		}
		size = int64(len(data)) - size
	}

	dir := filepath.Dir(cpath)
	if _, err := os.Stat(dir); err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return errors.Wrap(err, "move file to cache error")
		}
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	free, err := utils.GetDirFreeSpace(dir)
	if err != nil {
		return errors.Wrap(err, "save file to cache error")
	}
	if c.usedSpace+size > c.cacheSpace || int64(free) < size {
		if !c.cacheSwapout(size) {
			return errors.Wrap(errors.New("not enough cache space"), "save file to cache error")
		}
	}
	file, err := os.Create(cpath)
	if err != nil {
		return errors.Wrap(err, "save file to cache error")
	}
	_, err = file.Write(data)
	if err != nil {
		return errors.Wrap(err, "save file to cache error")
	}
	c.cacher.Add(fname, c.exp, CacheRecord{Cpath: cpath, Csize: size})
	c.usedSpace += size
	return nil
}

func (c *Cacher) AddCacheRecord(fname, cpath string) error {
	if cpath == "" {
		cpath = path.Join(c.CacheDir, fname)
	}
	f, err := os.Stat(cpath)
	if err != nil {
		return errors.Wrap(err, "add cache record error")
	}
	if f.Size() <= 0 {
		return errors.Wrap(errors.New("invalid file"), "add cache record error")
	}
	c.cacher.Add(fname, c.exp, CacheRecord{Cpath: cpath, Csize: f.Size()})
	return nil
}

func (c *Cacher) AddEmptyCacheRecord(fname string) {
	c.cacher.Add(fname, c.exp, CacheRecord{})
}

func (c *Cacher) GetCacheRecord(fname string) (string, error) {
	value, err := c.cacher.Value(fname)
	if err != nil {
		return "", errors.Wrap(err, "get cache record error")
	}
	item, ok := value.Data().(CacheRecord)
	if !ok {
		return "", errors.Wrap(err, "get cache record error")
	}
	return item.Cpath, nil
}

func (c *Cacher) GetCacheItem(fname string) (CacheItem, error) {
	value, err := c.cacher.Value(fname)
	if err != nil {
		return nil, errors.Wrap(err, "get cache record error")
	}
	return value, nil
}

func (c *Cacher) TraverseCache(f ForEachItems) {
	c.cacher.Foreach(
		func(key interface{}, item *cache2go.CacheItem) {
			f(key, item)
		},
	)
}

func (c *Cacher) RemoveCacheRecord(fname string) error {
	if !c.cacher.Exists(fname) {
		return nil
	}
	value, err := c.cacher.Delete(fname)
	if err != nil {
		return errors.Wrap(err, "remove cache record error")
	}

	_, ok := value.Data().(CacheRecord)
	if !ok {
		return errors.Wrap(errors.New("bad cache record"), "remove cache record error")
	}
	return nil
}

func (c *Cacher) removeFile(fpath string) error {
	f, err := os.Stat(fpath)
	if err != nil {
		return errors.Wrap(err, "remove cached file error")
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if f.Size() > c.usedSpace {
		return errors.Wrap(errors.New("bad file size"), "remove cached file error")
	}
	c.usedSpace -= f.Size()

	err = os.Remove(fpath)
	if err != nil {
		return errors.Wrap(err, "remove cached file error")
	}
	return nil
}

func (c *Cacher) FlushAndCleanCache(wantSize int64) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if float64(c.usedSpace)/float64(c.cacheSpace) < 0.75 {
		return false
	}
	return c.cacheSwapout(wantSize)
}
func (c *Cacher) cacheSwapout(size int64) bool {
	dqSize, delSize := int64(0), size-(c.cacheSpace-c.usedSpace)
	delQueue := make([]*cache2go.CacheItem, 0)
	if delSize <= 0 {
		return false
	}
	if c.cacheSpace-size <= 0 || c.usedSpace-size <= 0 {
		return false
	}
	c.cacher.Foreach(func(key interface{}, item *cache2go.CacheItem) {
		v, ok := item.Data().(CacheRecord)
		if !ok {
			return
		}
		c.insertNode(&delQueue, item)
		if dqSize < delSize {
			dqSize += v.Csize
		} else {
			sv := delQueue[len(delQueue)-1].Data().(CacheRecord)
			if dqSize-sv.Csize >= delSize {
				delQueue = delQueue[:len(delQueue)-1]
				dqSize -= sv.Csize
			}
		}
	})

	if dqSize < delSize {
		return false
	}

	for _, item := range delQueue {
		value, err := c.cacher.Delete(item.Key())
		if err != nil {
			return false
		}
		v, ok := value.Data().(CacheRecord)
		if !ok {
			return false
		}
		err = os.Remove(v.Cpath)
		if err != nil {
			return false
		}
		c.usedSpace -= v.Csize
	}

	return true
}

func (c *Cacher) insertNode(queue *([]*cache2go.CacheItem), elem *cache2go.CacheItem) {
	i, length := 0, len(*queue)
	for i = 0; i < length; i++ {
		tv := time.Since(elem.AccessedOn()) - time.Since((*queue)[i].AccessedOn())
		if tv > c.exp/6 ||
			(tv > 0 && elem.AccessCount() < (*queue)[i].AccessCount()) {
			break
		}
	}
	*queue = append(*queue, &cache2go.CacheItem{})
	copy((*queue)[i+1:length+1], (*queue)[i:length])
	(*queue)[i] = elem
}
