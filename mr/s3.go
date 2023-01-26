package mr

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

type s3Bucket struct {
	client *s3.S3
	name string
}

func newS3Bucket(region, bucket string) *s3Bucket {
	err := godotenv.Load("../.env")
	if err != nil {
		ErrorLogger.Fatal("Error loading .env file")
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
		Credentials: credentials.NewStaticCredentials(
			os.Getenv("S3_ACCESS_KEY"), 
			os.Getenv("S3_SECRET"), 
			""),
	})
	if err != nil {
		ErrorLogger.Fatal(err)
	}

	return &s3Bucket{
		client: s3.New(sess),
		name: bucket,
	}
}

func (bu *s3Bucket) uploadFile(filename string) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	_, err = bu.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bu.name),
		Key: aws.String(filename),
		Body: bytes.NewReader(file),
		ContentType: aws.String("text/plain"),
	})

	if err != nil {
		return err
	}

	InfoLogger.Printf("Successfully uploaded %s to S3.\n", filename)
	return nil
}

func (bu *s3Bucket) downloadFile(filename string) (*os.File, error) {
	res, err := bu.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bu.name),
		Key: aws.String(filename),
	})
	if err != nil {
		ErrorLogger.Fatal(err)
	}
	
	file, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
	if err != nil {
		return file, err
	}

	_, err = io.Copy(file, res.Body)
	if err != nil {
		return file, err
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return file, err
	}

	InfoLogger.Printf("Successfully downloaded %s from S3.\n", filename)
	return file, nil
}

func (bu *s3Bucket) removeFiles() {
	InfoLogger.Println("Removing S3 files...")

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bu.name),
	}

	res, err := bu.client.ListObjectsV2(params)
	if err != nil {
		ErrorLogger.Fatal(err)
	}

	if len(res.Contents) > 0 {
		var objects []*s3.ObjectIdentifier
		for _, item := range res.Contents {
			objects = append(objects, &s3.ObjectIdentifier{Key: item.Key})
		}

		deleteParams := &s3.DeleteObjectsInput{
			Bucket: aws.String(bu.name),
			Delete: &s3.Delete{Objects: objects},
		}

		_, err = bu.client.DeleteObjects(deleteParams)
		if err != nil {
			ErrorLogger.Fatal(err)
		}

		InfoLogger.Println("Done.")
	} else {
		InfoLogger.Println("No files in bucket.")
	}
}
