package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type AwsVpcFlowLog struct {
	Version       int    `gorm:"type:int" json:"version"`
	AccountId     string `gorm:"size:20" json:"account_id"`
	InterfaceId   string `gorm:"size:20" json:"interface_id"`
	SrcAddr       string `gorm:"size:20" json:"srcaddr"`
	DstAddr       string `gorm:"size:20" json:"dstaddr"`
	SrcPort       int    `gorm:"type:int" json:"srcport"`
	DstPort       int    `gorm:"type:int" json:"dstport"`
	Protocol      int    `gorm:"type:int" json:"protocol"`
	Packets       int    `gorm:"type:int" json:"packets"`
	Bytes         int    `gorm:"type:int" json:"bytes"`
	Start         int    `gorm:"type:int" json:"start"`
	End           int    `gorm:"type:int" json:"end"`
	Action        string `gorm:"size:20" json:"action"`
	LogStatus     string `gorm:"size:20" json:"log_status"`
	FlowDirection string `gorm:"size:20" json:"flow_direction"`
}

func loadFlowLog(rd io.Reader) {

	reader, _ := gzip.NewReader(rd)
	defer reader.Close()

	r := bufio.NewReader(reader)
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		row := scanner.Text()
		listString := strings.Split(row, " ")
		var log AwsVpcFlowLog
		for i, str := range listString {
			v := reflect.ValueOf(&log).Elem()
			fieldVal := v.Field(i)
			if fieldVal.Kind() == reflect.String {
				fieldVal.SetString(str)
				continue
			}
			val, _ := strconv.ParseInt(str, 10, 32)
			fieldVal.Set(reflect.ValueOf(int(val)))

		}

		fmt.Println(log)
	}
}

func GetGZObjectFromLocal(fn string) *os.File {
	fr, err := os.Open(fn)
	if err != nil {
		panic(err)
	} else {
		println("open file success!")
	}
	return fr

}

func main() {

	accountId := "007674436253"
	regionCode := "ap-south-1"
	fileName := fmt.Sprintf("%s_vpcflowlogs_%s_fl-0238b8bb5e5437d5a_20210915T0220Z_eb26a11e.log.gz", accountId, regionCode)
	rd := GetGZObjectFromLocal(fileName)
	loadFlowLog(rd)

}
