package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var picMap = map[string]string{
	"jpg": "FFD8",
	"bmp": "424D",
	"png": "8950",
}

func main() {
	// todo 读取文件判断
	cpu := runtime.NumCPU()
	runtime.GOMAXPROCS(cpu / 8)
	dirPath := `E:\Image\2021-10`
	saveDir := `D:\Image2\2021-10`
	ScanAllDir(dirPath, saveDir)
}

// ScanAllDir 整个文件夹内的一级dat文件全部转换
func ScanAllDir(srcDirPath, destDirPath string) {
	dir, _ := ioutil.ReadDir(srcDirPath)

	_, err := os.Stat(destDirPath)
	if os.IsNotExist(err) {
		err = os.Mkdir(destDirPath, os.ModePerm)
		if err != nil {
			fmt.Println(err)
		}
		dir, _ = ioutil.ReadDir(srcDirPath)
	}

	wg := sync.WaitGroup{}

	for _, info := range dir {
		if !info.IsDir() {
			wg.Add(1)
			go func(file string, wg *sync.WaitGroup) {
				decode := NewWxImageDecode(srcDirPath+"\\"+file, destDirPath)
				decode.DecodeImage()
				wg.Done()
			}(info.Name(), &wg)
		}
	}
	time.Sleep(5 * time.Second)
	wg.Wait()
}

type WxImageDecode struct {
	code        int64
	PicType     string
	SrcFilePath string
	DestPath    string
}

func NewWxImageDecode(src, dest string) *WxImageDecode {
	return &WxImageDecode{
		code:        0,
		SrcFilePath: src,
		DestPath:    dest,
	}
}

func (w *WxImageDecode) DecodeImage() {
	output := w.DestPath

	stat, err := os.Stat(output)

	// 不存在则，创建文件夹
	if os.IsNotExist(err) {
		err = os.Mkdir(output, os.ModePerm)
		if err != nil {
			panic(err)
		}
	} else if err != nil {
		log.Println(err)
		panic(err)
	}

	// 判断是否是文件夹，不是文件夹，则创建
	if !stat.IsDir() {
		err = os.Mkdir(output, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}

	// 打开源文件
	f, err := os.OpenFile(w.SrcFilePath, os.O_RDONLY, os.ModePerm)
	defer f.Close()
	if err != nil {
		log.Println("读取文件错误", err.Error())
		return
	}

	// 获取文件信息
	info, _ := f.Stat()

	buff := make([]byte, 2)
	_, err = f.Read(buff)
	if err != nil {
		panic(err)
	}

	// 未encode
	parseInt, _ := strconv.ParseInt(hex.EncodeToString(buff), 16, 32)
	hexI := fmt.Sprintf("%x", parseInt)
	for k, v := range picMap {
		if v == strings.ToUpper(hexI) {
			w.PicType = k
			file, _ := ioutil.ReadFile(w.SrcFilePath)
			err := os.WriteFile(w.GetSaveFileName(info.Name()), file, os.ModePerm)
			if err != nil {
				fmt.Println("直接保存文件错误：", err)
				return
			}
			return
		}
	}

	// 检测图片格式
	for k, v := range picMap {
		w.PicType = k
		// 解码
		w.decode(buff)
		var testStr string
		for _, b := range buff {
			parseInt, _ := strconv.ParseInt(hex.EncodeToString([]byte{b}), 16, 16)
			i := parseInt ^ w.code
			testStr += fmt.Sprintf("%x", i)
		}

		//log.Println(testStr, "=====", v)
		if strings.ToUpper(testStr) == v {
			break
		}
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		log.Println(w.SrcFilePath, "读取发生错误：", "")
		return
	}

	destPic, err := os.OpenFile(w.DestPath+"\\"+info.Name()+"."+w.PicType, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	defer destPic.Close()
	if err != nil {
		log.Println("保存失败 :", err)
		return
	}

	// 写入文件
	buff = make([]byte, 8192)
	wBuff := make([]byte, 8192)
	for {
		size, _ := f.Read(buff)
		for i, v := range buff {
			si, _ := strconv.ParseInt(hex.EncodeToString([]byte{v}), 16, 16)
			xori := si ^ w.code
			wBuff[i] = w.IntToByte(xori)[7]
		}
		_, err = destPic.Write(wBuff)
		if err != nil {
			log.Println("写入失败：", err)
			return
		}
		if size < 8192 {
			break
		}
	}
}

// decode 获取加密字节码
func (w *WxImageDecode) decode(buff []byte) {
	// png 8950 jpg jpeg FFD8
	picCode := picMap[w.PicType]
	fmt.Println(w.PicType)
	img, _ := strconv.ParseInt(picCode, 16, 32)
	first2b, _ := strconv.ParseInt(hex.EncodeToString(buff), 16, 32)
	// 异或运算 解码
	code := first2b ^ img

	str := fmt.Sprintf("%x", code)
	fmt.Println(len(str), str, code, len(buff), first2b, img)
	w.code, _ = strconv.ParseInt(str[:2], 16, 16)
}

// ByteToInt bytes转Int64
func (w *WxImageDecode) ByteToInt(bs []byte) (i int64) {
	bytesBuf := bytes.NewBuffer(bs)
	binary.Read(bytesBuf, binary.BigEndian, &i)
	return
}

// IntToByte 整型转字节
func (w *WxImageDecode) IntToByte(i int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	binary.Write(buffer, binary.BigEndian, i)
	return buffer.Bytes()
}

// GetSaveFileName 文件保存时的文件名
func (w *WxImageDecode) GetSaveFileName(name string) string {
	return w.DestPath + "\\" + name + "." + w.PicType
}
