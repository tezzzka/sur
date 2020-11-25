package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
)

type storage struct {
	Rates struct {
		USD float64 `json:"USD"`
	} `json:"rates"`
}

func Uploader(filename string) {
	ctx := context.Background()
	endpoint := "http://10.0.100.144:9000"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Make a new bucket called mymusic.
	bucketName := "csv-data"
	location := "us-east-1"

	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}

	objectName := "backup.csv"
	filePath := "./"
	contentType := "application/zip"

	// Upload the zip file with FPutObject
	n, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)
}

func find(source []string, value string) bool {
	for _, item := range source {
		if item == value {
			return true
		}
	}
	return false
}

func Pipe(rate float64) {
	//В задании сказано, что программа должна срабатывать за прошлый период (прошлый день)
	t := (time.Now().AddDate(0, 0, -1).Format("2006-01-02"))
	t += ".csv"

	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		source := strings.Split(f.Name(), "_")
		result := find(source, t)
		if result == true {
			fmt.Println(source[2])
			if source[2] == t {
				if Modifier(f.Name(), rate) == true {
					fmt.Println("Программа завершена.")
				}
			}
		}
	}

}

func Modifier(filename string, rate float64) bool {
	sourcefile, err := os.Open(filename)

	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	sourceFileHandle := csv.NewReader(sourcefile)
	lines, err := sourceFileHandle.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	//Математика.
	for key := 0; key < len(lines); key++ {
		b := strings.Split(lines[key][0], ",")
		if s, err := strconv.ParseFloat(b[2], 64); err == nil {
			res := fmt.Sprintf("%f", s*rate)
			lines[key][0] += "," + res
		}
	}
	// fmt.Println(lines)
	sourcefile.Close()
	destfile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer destfile.Close()
	destfileHandle := csv.NewWriter(destfile)
	destfileHandle.WriteAll(lines)
	destfileHandle.Flush()

	return true
}

func GetRates(url, apiKey string) []byte {
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	request.Header.Set("X-Auth-Token", apiKey)

	var client = http.Client{}
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()

	jsonByte, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}

	return jsonByte

}

func main() {
	url := "https://api.exchangeratesapi.io/2020-10-28?base=RUB&symbo"
	bin := GetRates(url, "test")
	var data storage
	err := json.Unmarshal(bin, &data)
	if err != nil {
		panic(err)
	}
	//Pipe(data.Rates.USD)
	Uploader("backup.csv")

}
