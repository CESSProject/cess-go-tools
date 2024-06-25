package cansproto

import (
	//"archive/tar"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mholt/archiver"

	"github.com/pkg/errors"
)

const (
	ARCHIVER_FORMAT_ZIP   = "zip"
	ARCHIVER_FORMAT_TAR   = "tar"
	ARCHIVER_FORMAT_TARGZ = "tar.gz"

	CANS_PROTO_FLAG         = "CANS"
	CANS_PROTO_FLAG_LEN     = 4
	METADATA_LEN_BYTES_LEN  = 4 // max bytes number <= 2^(4*8)
	DEFAULT_CAN_SIZE        = 32 * 1024 * 1024
	CIPHER_CAN_SIZE         = 32*1024*1024 - 16
	CAN_FILE_META_BYTES_LEN = 48
	PROTO_VERSION           = "v0.1"
)

type CanMetadata struct {
	TotalFileSize     uint64 `json:"total_file_size"`
	TotalArchivedSize uint64 `json:"total_archived_size"`
	FileNum           int    `json:"file_num"`
	ArchiveFormat     string `json:"archive_format"`
	Files             []File `json:"files"`
}

type File struct {
	FileName string `json:"file_name"`
	FileSize uint64 `json:"file_size"`
	IsSplit  bool   `json:"is_split"`
}

type BoxMetadata struct {
	FileName          string        `json:"file_name"`
	TotalFileSize     uint64        `json:"total_file_size"`
	TotalArchivedSize uint64        `json:"total_archived_size"`
	ArchiveFormat     string        `json:"archive_format"`
	FileNum           int           `json:"file_num"`
	CanNum            int           `json:"can_num"`
	IsSplit           bool          `json:"is_split"`
	Cans              []CanMetadata `json:"cans"`
	Version           string        `json:"version"`
}

type CanBox struct {
	Metadata     BoxMetadata
	Fils         []string
	SourceSize   []int64
	CompressSize []int64
}

type Archiver interface {
	Archive(files []string, dest string) error
	Unarchive(src, dest string) error
	Extract(src string, target string, dest string) error
	Close() error
}

func ArchiveCanFile(filesDir, filename, archiveFormat string, encrypt, fileBeSplit bool, nameConv func(string) string, ar Archiver) error {
	box := CanBox{
		Metadata: BoxMetadata{
			FileName:      filename,
			IsSplit:       fileBeSplit,
			Version:       PROTO_VERSION,
			ArchiveFormat: archiveFormat,
		},
	}
	entries, err := os.ReadDir(filesDir)
	sort.Sort(DirEntries(entries))
	if err != nil {
		return errors.Wrap(err, "archive can file error")
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fpath := filepath.Join(filesDir, nameConv(entry.Name()))
		box.Fils = append(box.Fils, fpath)
		box.Metadata.FileNum++
		info, err := entry.Info()
		if err != nil {
			return errors.Wrap(err, "archive can file error")
		}
		box.Metadata.TotalFileSize += uint64(info.Size())
		box.SourceSize = append(box.SourceSize, info.Size())
		err = os.Rename(filepath.Join(filesDir, entry.Name()), fpath)
		if err != nil {
			return errors.Wrap(err, "archive can file error")
		}
	}

	if ar == nil && archiveFormat != "" {
		if ar, err = NewArchiver(archiveFormat); err != nil {
			return errors.Wrap(err, "archive can file error")
		}
	}
	if archiveFormat != "" {
		for i := 0; i < len(box.Fils); i++ {
			newpath := fmt.Sprintf("%s.%s", box.Fils[i], archiveFormat)
			err = DataCompress(box.Fils[i:i+1], newpath, ar)
			if err != nil {
				return errors.Wrap(err, "archive can file error")
			}
			stat, err := os.Stat(newpath)
			if err != nil {
				return errors.Wrap(err, "archive can file error")
			}
			err = os.Remove(box.Fils[i])
			if err != nil {
				return errors.Wrap(err, "archive can file error")
			}
			box.Fils[i] = newpath
			box.Metadata.TotalArchivedSize += uint64(stat.Size())
			box.CompressSize = append(box.CompressSize, stat.Size())
		}
	}

	var canSize uint64 = DEFAULT_CAN_SIZE
	if encrypt {
		canSize = CIPHER_CAN_SIZE
	}

	if err = FillingCans(&box, fileBeSplit, canSize); err != nil {
		return errors.Wrap(err, "archive can file error")
	}

	err = SealCans(&box, filesDir, filename, canSize)
	return errors.Wrap(err, "archive can file error")
}

func FillingCans(box *CanBox, fileBeSplit bool, canSize uint64) error {
	var (
		avgMetaSize int64   = 256 * 1024
		sizeList    []int64 = box.CompressSize
	)
	if len(box.CompressSize) == 0 {
		sizeList = box.SourceSize
	}

	canUpdate := func(can *CanMetadata, i int) {
		can.FileNum++
		can.Files = append([]File{{
			FileName: filepath.Base(box.Fils[i]),
			FileSize: uint64(sizeList[i]),
		}}, can.Files...)
		can.TotalArchivedSize += uint64(sizeList[i])
		can.TotalFileSize += uint64(box.SourceSize[i])
	}

	isSplit := false
	idxRecord := 0
	for idx := len(box.Fils) - 1; idx >= 0; {
		can := CanMetadata{
			ArchiveFormat: box.Metadata.ArchiveFormat,
		}
		i := idx
		for ; i >= 0; i-- {
			idxRecord = i
			if can.TotalArchivedSize+uint64(sizeList[i]+avgMetaSize) > canSize {
				canCopy := can
				canUpdate(&canCopy, i)
				jbyte, err := json.Marshal(canCopy)
				if err != nil {
					return err
				}
				newMetaSize := CAN_FILE_META_BYTES_LEN + CANS_PROTO_FLAG_LEN +
					METADATA_LEN_BYTES_LEN + uint64(len(jbyte))
				avgMetaSize = (avgMetaSize + int64(newMetaSize)) / 2
				if can.TotalArchivedSize+newMetaSize+uint64(sizeList[i]) >= canSize {
					if !fileBeSplit {
						break
					}
					isSplit = true
				}
			}
			canUpdate(&can, i)
			if isSplit {
				jbyte, err := json.Marshal(can)
				if err != nil {
					return err
				}
				size := canSize -
					(can.TotalArchivedSize - uint64(sizeList[i]) +
						uint64(len(jbyte)) + CANS_PROTO_FLAG_LEN + METADATA_LEN_BYTES_LEN)
				if size <= 0 {
					can.FileNum--
					can.Files = can.Files[1:]
					can.TotalArchivedSize -= uint64(sizeList[i])
					can.TotalFileSize -= uint64(box.SourceSize[i])
				} else if size < uint64(sizeList[i]) {
					can.Files[0].FileSize = size
					can.Files[0].IsSplit = true
					if len(box.CompressSize) > 0 {
						box.SourceSize[i] -= box.SourceSize[i] * int64(size) / sizeList[i]
					}
					sizeList[i] -= int64(size)
					can.TotalArchivedSize -= uint64(sizeList[i])
					can.TotalFileSize -= uint64(box.SourceSize[i])
				} else {
					i--
					can.Files[0].IsSplit = isSplit
				}
				break
			}
		}

		if idxRecord != i {
			isSplit = false
		}

		if can.TotalArchivedSize == 0 {
			return errors.New("file size exceeds can capacity")
		}
		box.Metadata.Cans = append([]CanMetadata{can}, box.Metadata.Cans...)
		box.Metadata.CanNum++
		box.Metadata.FileNum += can.FileNum
		box.Metadata.TotalArchivedSize = can.TotalArchivedSize
		box.Metadata.TotalFileSize = can.TotalFileSize
		idx = i
	}

	boxMetaBytes, err := json.Marshal(box.Metadata)
	if err != nil {
		return err
	}
	can0MetaBytes, err := json.Marshal(box.Metadata.Cans[0])
	if err != nil {
		return err
	}
	size0 := uint64(CANS_PROTO_FLAG_LEN+METADATA_LEN_BYTES_LEN*2) +
		uint64(len(boxMetaBytes)+len(can0MetaBytes)) + box.Metadata.Cans[0].TotalArchivedSize
	if size0 > canSize {
		box.Metadata.Cans = append([]CanMetadata{{}}, box.Metadata.Cans...)
		box.Metadata.CanNum++
	}
	return nil
}

func SealCans(box *CanBox, fdir, fname string, canSize uint64) error {

	buf := bytes.NewBuffer(make([]byte, 0, canSize))
	boxMetaBytes, err := json.Marshal(box.Metadata)
	if err != nil {
		return err
	}
	fpath := filepath.Join(fdir, fname)
	sealFile, err := os.Create(fpath)
	if err != nil {
		return err
	}
	defer sealFile.Close()
	counter := -1
	sizeRecord := uint64(0)
	nameRecord := ""
	for i := 0; i < len(box.Metadata.Cans); i++ {

		buf.Write([]byte(CANS_PROTO_FLAG))
		canMetaBytes, err := json.Marshal(box.Metadata.Cans[i])
		if err != nil {
			return err
		}
		if i == 0 {
			buf.Write(Uint32ToBytes(uint32(len(boxMetaBytes))))
			buf.Write(Uint32ToBytes(uint32(len(canMetaBytes))))
			buf.Write(boxMetaBytes)
			buf.Write(canMetaBytes)
		} else {
			buf.Write(Uint32ToBytes(uint32(len(canMetaBytes))))
			buf.Write(canMetaBytes)
		}
		for _, cfile := range box.Metadata.Cans[i].Files {
			if err = func() error {
				if nameRecord != cfile.FileName {
					counter++
					sizeRecord = 0
				}
				nameRecord = cfile.FileName
				f, err := os.Open(box.Fils[counter])
				if err != nil {
					return err
				}
				defer f.Close()
				fbuf := make([]byte, cfile.FileSize)
				n, err := f.ReadAt(fbuf, int64(sizeRecord))
				if err != nil {
					return err
				}
				sizeRecord += cfile.FileSize
				if uint64(n) != cfile.FileSize {
					return fmt.Errorf("read file %s error: the sub file size doesn't match", box.Fils[counter])
				}
				n, err = buf.Write(fbuf)
				if err != nil {
					return err
				}
				if uint64(n) != cfile.FileSize {
					return fmt.Errorf("write file %s error: the sub file size doesn't match", box.Fils[counter])
				}
				return nil
			}(); err != nil {
				return err
			}
		}
		if uint64(buf.Len()) < canSize {
			buf.Write(make([]byte, canSize-uint64(buf.Len())))
		}
		n, err := buf.WriteTo(sealFile)
		if err != nil {
			return err
		}
		if uint64(n) != canSize {
			return fmt.Errorf("can file size doesn't match, expected %d, actual %d", canSize, n)
		}
	}

	return nil
}

func ParseCanMetadata(fbytes []byte, canID int) (CanMetadata, int, error) {
	var (
		metaLen uint32
		meta    CanMetadata
		start   int
	)
	if string(fbytes[:CANS_PROTO_FLAG_LEN]) != CANS_PROTO_FLAG {
		err := errors.New("this file does not support the cans-protocol")
		return meta, 0, errors.Wrap(err, "parse can metadata error")
	}

	if canID == 0 {
		start = CANS_PROTO_FLAG_LEN + METADATA_LEN_BYTES_LEN
		boxMetaLen := binary.BigEndian.Uint32(fbytes[CANS_PROTO_FLAG_LEN:start])
		metaLen = binary.BigEndian.Uint32(fbytes[start : start+METADATA_LEN_BYTES_LEN])
		start = start + METADATA_LEN_BYTES_LEN + int(boxMetaLen)
	} else {
		start = CANS_PROTO_FLAG_LEN
		metaLen = binary.BigEndian.Uint32(fbytes[start : start+METADATA_LEN_BYTES_LEN])
		start = start + METADATA_LEN_BYTES_LEN
	}
	if len(fbytes) < start+int(metaLen) {
		err := errors.New("bad can file size, unable to fit the metadata size")
		return meta, 0, errors.Wrap(err, "parse can metadata error")
	}

	err := json.Unmarshal(fbytes[start:start+int(metaLen)], &meta)
	if err != nil {
		return meta, 0, errors.Wrap(err, "parse can metadata error")
	}
	return meta, start + int(metaLen), nil
}

func ParseCanBoxMetadata(fbytes []byte) (BoxMetadata, error) {
	var meta BoxMetadata

	if string(fbytes[:CANS_PROTO_FLAG_LEN]) != CANS_PROTO_FLAG {
		err := errors.New("this file does not support the cans-protocol")
		return meta, errors.Wrap(err, "parse can metadata error")
	}
	start := CANS_PROTO_FLAG_LEN
	metaLen := binary.BigEndian.Uint32(fbytes[start : start+METADATA_LEN_BYTES_LEN])
	start = start + METADATA_LEN_BYTES_LEN*2
	if len(fbytes) < start+int(metaLen) {
		err := errors.New("bad can file size, unable to fit the metadata size")
		return meta, errors.Wrap(err, "parse can metadata error")
	}
	err := json.Unmarshal(fbytes[start:start+int(metaLen)], &meta)
	if err != nil {
		return meta, errors.Wrap(err, "parse can box metadata error")
	}
	return meta, nil
}

func ParseData(src, destDir string, canID int, ar Archiver) error {
	fbytes, err := os.ReadFile(src)
	if err != nil {
		return errors.Wrap(err, "parse data from can error")
	}
	meta, start, err := ParseCanMetadata(fbytes, canID)
	if err != nil {
		return errors.Wrap(err, "parse data from can error")
	}

	if ar == nil && meta.ArchiveFormat != "" {
		ar, err = NewArchiver(meta.ArchiveFormat)
		if err != nil {
			return errors.Wrap(err, "parse data from can error")
		}
	}

	for i := 0; i < len(meta.Files); i++ {
		if meta.Files[i].IsSplit {
			continue
		}
		fpath := filepath.Join(destDir, meta.Files[i].FileName)
		err = os.WriteFile(fpath, fbytes[start:start+int(meta.Files[i].FileSize)], 0755)
		if err != nil {
			return errors.Wrap(err, "parse data from can error")
		}
		if ar != nil {
			err = ar.Unarchive(fpath, destDir)
			if err != nil {
				return errors.Wrap(err, "parse data from can error")
			}
			err = os.Remove(fpath)
			if err != nil {
				return errors.Wrap(err, "parse data from can error")
			}
		}
		start += int(meta.Files[i].FileSize)
	}
	return nil
}

func ExtractFileFromCan(src, destDir, target string, canID int, ar Archiver) (string, error) {
	fbytes, err := os.ReadFile(src)
	if err != nil {
		return "", errors.Wrap(err, "extract data from can error")
	}
	meta, start, err := ParseCanMetadata(fbytes, canID)
	if err != nil {
		return "", errors.Wrap(err, "extract data from can error")
	}

	if ar == nil && meta.ArchiveFormat != "" {
		ar, err = NewArchiver(meta.ArchiveFormat)
		if err != nil {
			return "", errors.Wrap(err, "extract data from can error")
		}
	}
	var targetFile *File
	for _, file := range meta.Files {
		if strings.Contains(file.FileName, target) {
			targetFile = &file
			break
		}
		start += int(file.FileSize)
	}
	if targetFile == nil {
		err := errors.New("target file not found.")
		return "", errors.Wrap(err, "extract data from can error")
	}

	fpath := filepath.Join(destDir, targetFile.FileName)
	err = os.WriteFile(fpath, fbytes[start:start+int(targetFile.FileSize)], 0755)
	if err != nil {
		return "", errors.Wrap(err, "extract data from can error")
	}
	if ar != nil && !targetFile.IsSplit {
		err = ar.Unarchive(fpath, destDir)
		if err != nil {
			return "", errors.Wrap(err, "extract data from can error")
		}
		err = os.Remove(fpath)
		if err != nil {
			return "", errors.Wrap(err, "extract data from can error")
		}
		fpath = strings.TrimSuffix(fpath, filepath.Ext(fpath))
	}
	return fpath, nil
}

func PickUpSplitFile(srcs []string, destDir, target string, startCanID int, ar Archiver) (string, error) {
	targetPath := filepath.Join(destDir, strconv.FormatInt(time.Now().Unix(), 10))
	f, err := os.Create(targetPath)
	if err != nil {
		return "", errors.Wrap(err, "pick up split data from can error")
	}
	fname := ""
	for i, src := range srcs {
		fpath, err := ExtractFileFromCan(src, destDir, target, startCanID+i, nil)
		if err != nil {
			f.Close()
			return "", errors.Wrap(err, "pick up split data from can error")
		}
		if err = func() error {
			sf, err := os.Open(fpath)
			if err != nil {
				return err
			}
			defer sf.Close()
			_, err = io.Copy(f, sf)
			if err != nil {
				return err
			}
			return nil
		}(); err != nil {
			f.Close()
			return "", errors.Wrap(err, "pick up split data from can error")
		}
		os.Remove(fpath)
		fname = fpath
	}
	f.Close()
	if err = os.Rename(targetPath, fname); err != nil {
		return "", errors.Wrap(err, "pick up split data from can error")
	}
	if ar == nil {
		ar, err = NewArchiver(strings.TrimPrefix(filepath.Ext(fname), "."))
		if err != nil {
			return fname, nil
		}
	}
	if ar != nil {
		err = ar.Unarchive(fname, destDir)
		if err != nil {
			return "", errors.Wrap(err, "pick up split data from can error")
		}
		fname = strings.TrimSuffix(fname, filepath.Ext(fname))
	}
	return fname, nil
}

func Uint32ToBytes(data uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(data))
	return buf
}

func NewArchiver(archiveFormat string) (Archiver, error) {
	var ar Archiver
	switch archiveFormat {
	case ARCHIVER_FORMAT_ZIP:
		ar = archiver.NewZip()
	case ARCHIVER_FORMAT_TAR:
		ar = archiver.NewTar()

	case ARCHIVER_FORMAT_TARGZ:
		ar = archiver.NewTarGz()
	default:
		err := errors.New("unsupported archive format")
		return nil, errors.Wrap(err, "compress data error")
	}
	return ar, nil
}

func DataCompress(files []string, dest string, ar Archiver) error {
	err := ar.Archive(files, dest)
	if err != nil {
		return errors.Wrap(err, "compress data error")
	}
	return nil
}

func DataDeCompress(src, dest string, ar Archiver) error {
	err := ar.Unarchive(src, dest)
	if err != nil {
		return errors.Wrap(err, "decompress data error")
	}
	return nil
}

func DataExtract(src, dest, target string, ar Archiver) error {
	err := ar.Extract(src, target, dest)
	if err != nil {
		return errors.Wrap(err, "extract data error")
	}
	return nil
}

type DirEntries []fs.DirEntry

func (d DirEntries) Len() int { return len(d) }
func (d DirEntries) Less(i, j int) bool {

	si := strings.Split(d[i].Name(), "-")
	sj := strings.Split(d[j].Name(), "-")
	ni, err := strconv.Atoi(si[len(si)-1])
	if err != nil {
		return false
	}
	nj, err := strconv.Atoi(sj[len(sj)-1])
	if err != nil {
		return false
	}
	return ni < nj
}

func (d DirEntries) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
