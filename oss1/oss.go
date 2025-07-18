package oss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/distribution/distribution/v3/internal/dcontext"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	driverName         = "oss"
	minChunkSize       = 1024 * 1024
	chunkSize          = 10 * 1024 * 1024
	contentType        = "application/octet-stream"
	listMax      int32 = 1000

	// multipartCopyThresholdSize defines the default object size
	// above which multipart copy will be used. (Copy Object - Copy is used
	// for objects at or below this size.)
	// Empirically, 32 MB is optimal. Reference from S3 driver
	multipartCopyThresholdSize = 32 * 1024 * 1024

	// default chunk size for multipart copy
	multipartCopyChunkSize = 32 * 1024 * 1024

	// defines the default maximum number of concurrent upload part
	multipartCopyMaxThread = 100
)

var _ storagedriver.StorageDriver = &driver{}

// driver is the core service for interacting with OSS
type driver struct {
	oss           *oss.Client
	bucket        string
	rootDirectory string
	chunkSize     int64
	sessions      sync.Map
}

type baseEmbed struct {
	base.Base
}

// Driver implements the storagedriver.StorageDriver interface
type Driver struct {
	baseEmbed
}

func init() {
	factory.Register(driverName, &ossDriverFactory{})
}

// ossDriverFactory is the factory for creating new driver
type ossDriverFactory struct{}

func (f *ossDriverFactory) Create(
	ctx context.Context,
	parameters map[string]interface{},
) (storagedriver.StorageDriver, error) {
	params, err := NewParameters(parameters)
	if err != nil {
		return nil, err
	}
	return New(ctx, params)
}

func New(ctx context.Context, params *Parameters) (*Driver, error) {
	// initialize OSS oss
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			params.AccessKeyID, params.AccessKeySecret, "")).
		WithRegion(params.Region)

	client := oss.NewClient(cfg)

	d := &driver{
		oss:           client,
		bucket:        params.Bucket,
		chunkSize:     params.ChunkSize,
		rootDirectory: params.RootDirectory,
		sessions:      sync.Map{},
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func parseNotFoundError(path string, err error) error {
	var se *oss.ServiceError
	if errors.As(err, &se) {
		if se.StatusCode == http.StatusNotFound {
			return storagedriver.PathNotFoundError{Path: path}
		}
	}

	return err
}

// Implement the storagedriver.StorageDriver interface

// Name return the plugin name
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	_, err := d.oss.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.pathToKeyPtr(path),
		Body:   bytes.NewReader(content),
	})
	return err
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	req := &oss.GetObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.pathToKeyPtr(path),
		Range:  d.ossRangePtr(offset),
	}

	resp, err := d.oss.GetObject(ctx, req)
	if err != nil {
		var se *oss.ServiceError
		if errors.As(err, &se) {
			if se.StatusCode == http.StatusNotFound {
				return nil, storagedriver.PathNotFoundError{Path: path, DriverName: d.Name()}
			}
			if se.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				return io.NopCloser(bytes.NewReader(nil)), nil
			}
		}
		return nil, err
	}

	// Return IO ReadCloser directly
	return resp.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
// It only allows appending to paths with zero size committed content,
// in which the existing content is overridden with the new content.
// It returns storagedriver.Error when appending to paths
// with non-zero committed content.
func (d *driver) Writer(ctx context.Context, path string, appendMode bool) (storagedriver.FileWriter, error) {
	panic("not implement")
}

func (d *driver) statHead(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	resp, err := d.oss.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: d.ossBucketPtr(),
		Key:    d.pathToKeyPtr(path),
	})
	if err != nil {
		return nil, err
	}
	return &storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   false,
		Size:    resp.ContentLength,
		ModTime: *resp.LastModified,
	}, nil
}

// statList list objects, if existed, it is directory
func (d *driver) statList(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	resp, err := d.oss.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket:  d.ossBucketPtr(),
		Prefix:  d.pathToKeyPtr(path),
		MaxKeys: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Contents) == 1 {
		if *resp.Contents[0].Key != d.pathToKey(path) {
			return &storagedriver.FileInfoFields{
				Path:  path,
				IsDir: true,
			}, nil
		}
		return &storagedriver.FileInfoFields{
			Path:    path,
			Size:    resp.Contents[0].Size,
			ModTime: *resp.Contents[0].LastModified,
		}, nil
	}
	if len(resp.CommonPrefixes) == 1 {
		return &storagedriver.FileInfoFields{
			Path:  path,
			IsDir: true,
		}, nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fi, err := d.statHead(ctx, path)
	if err != nil || path == "/" { // is directory
		var se *oss.ServiceError
		if errors.As(err, &se) {
			fi, err := d.statList(ctx, path)
			if err != nil {
				return nil, err
			}
			return storagedriver.FileInfoInternal{FileInfoFields: *fi}, nil
		}
		return nil, err
	}
	return storagedriver.FileInfoInternal{FileInfoFields: *fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	if path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.pathToKey("") == "" {
		prefix = "/"
	}

	resp, err := d.oss.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket:    d.ossBucketPtr(),
		Prefix:    d.pathToKeyPtr(path),
		Delimiter: oss.Ptr("/"),
		MaxKeys:   listMax,
	})
	if err != nil {
		return nil, parseNotFoundError(opath, err)
	}

	var files []string
	var directories []string

	for {
		for _, key := range resp.Contents {
			files = append(files, strings.Replace(*key.Key, d.pathToKey(""), prefix, 1))
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			commonPrefix := *commonPrefix.Prefix
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.pathToKey(""), prefix, 1))
		}

		if !resp.IsTruncated {
			break
		}

		resp, err = d.oss.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
			Bucket:            d.ossBucketPtr(),
			Prefix:            d.pathToKeyPtr(path),
			Delimiter:         oss.Ptr("/"),
			MaxKeys:           listMax,
			ContinuationToken: resp.NextContinuationToken,
		})
		if err != nil {
			return nil, err
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	copier := d.oss.NewCopier()

	copyReq := &oss.CopyObjectRequest{
		Bucket:          d.ossBucketPtr(),
		Key:             d.pathToKeyPtr(destPath),
		SourceBucket:    d.ossBucketPtr(),
		SourceKey:       d.pathToKeyPtr(sourcePath),
		ForbidOverwrite: oss.Ptr("false"),
	}

	// Copy
	if _, err := copier.Copy(ctx, copyReq); err != nil {
		return err
	}

	// Delete the source file after copying
	if err := d.Delete(ctx, sourcePath); err != nil {
		return err
	}

	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
// We must be careful since S3 does not guarantee read after delete consistency
func (d *driver) Delete(ctx context.Context, path string) error {
	ossObjects := make([]oss.DeleteObject, 0, listMax)
	ossKey := d.pathToKey(path)
	listObjectsReq := &oss.ListObjectsV2Request{
		Bucket: d.ossBucketPtr(),
		Prefix: d.pathToKeyPtr(path),
	}

	for {
		// list all the objects
		resp, err := d.oss.ListObjectsV2(ctx, listObjectsReq)

		// resp.Contents can only be empty on the first call
		// if there were no more results to return after the first call, resp.IsTruncated would have been false
		// and the loop would exit without recalling ListObjects
		if err != nil || len(resp.Contents) == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		for _, obj := range resp.Contents {
			// Skip if we encounter a obj that is not a subpath (so that deleting "/a" does not delete "/ab").
			if len(*obj.Key) > len(ossKey) && (*obj.Key)[len(ossKey)] != '/' {
				continue
			}
			ossObjects = append(ossObjects, oss.DeleteObject{
				Key: obj.Key,
			})
		}

		// Delete objects only if the list is not empty, otherwise S3 API returns a cryptic error
		if len(ossObjects) > 0 {
			// NOTE: according to AWS docs https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
			// by default the response returns up to 1,000 obj names. The response _might_ contain fewer keys but it will never contain more.
			// 10000 keys is coincidentally (?) also the max number of keys that can be deleted in a single Delete operation, so we'll just smack
			// Delete here straight away and reset the object slice when successful.
			resp, err := d.oss.DeleteMultipleObjects(ctx, &oss.DeleteMultipleObjectsRequest{
				Bucket:  d.ossBucketPtr(),
				Objects: ossObjects,
			})
			if err != nil {
				return err
			}

			if len(resp.DeletedObjects) != len(ossObjects) {
				// NOTE: AWS SDK s3.Error does not implement error interface which
				// is pretty intensely sad, so we have to do away with this for now.
				errs := make([]error, 0, len(ossObjects)-len(resp.DeletedObjects))
				deletedMap := make(map[string]bool)
				for _, deleted := range resp.DeletedObjects {
					deletedMap[*deleted.Key] = true
				}

				for _, obj := range ossObjects {
					if !deletedMap[*obj.Key] {
						errs = append(errs, errors.New(fmt.Sprintf("NOT delete %s", *obj.Key)))
					}
				}

				return storagedriver.Errors{
					DriverName: driverName,
					Errs:       errs,
				}
			}
		}
		// NOTE: we don't want to reallocate
		// the slice so we simply "reset" it
		ossObjects = ossObjects[:0]

		// resp.Contents must have at least one element, or we would have returned not found
		listObjectsReq.StartAfter = resp.Contents[len(resp.Contents)-1].Key

		// from the s3 api docs, IsTruncated "specifies whether (true) or not (false) all the results were returned"
		// if everything has been returned, break
		if !resp.IsTruncated {
			break
		}
	}

	return nil
}

func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return "", nil
	}

	expireOption := func(po *oss.PresignOptions) {
		po.Expires = 20 * time.Minute
	}

	var presign *oss.PresignResult
	var err error
	switch r.Method {
	case http.MethodGet:
		presign, err = d.oss.Presign(context.Background(), oss.GetObjectRequest{
			Bucket: d.ossBucketPtr(),
			Key:    d.pathToKeyPtr(path),
		}, expireOption)
	case http.MethodHead:
		presign, err = d.oss.Presign(context.Background(), oss.HeadObjectRequest{
			Bucket: d.ossBucketPtr(),
			Key:    d.pathToKeyPtr(path),
		}, expireOption)
	default:
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return presign.URL, nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, from string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	walkOptions := &storagedriver.WalkOptions{}
	for _, o := range options {
		o(walkOptions)
	}

	var objectCount int64
	if err := d.doWalk(ctx, &objectCount, from, walkOptions.StartAfterHint, f); err != nil {
		return err
	}

	return nil
}

func (d *driver) doWalk(parentCtx context.Context, objectCount *int64, from, startAfter string, f storagedriver.WalkFn) error {
	var retError error
	// the most recent skip directory to avoid walking over undesirable files
	var prevSkipDir string

	req := &oss.ListObjectsV2Request{
		Bucket:     d.ossBucketPtr(),
		Prefix:     d.pathToKeyPtr(from),
		StartAfter: d.pathToKeyPtr(startAfter),
		MaxKeys:    1000, // max 1000
	}
	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("oss.ListObjectV2Request(%s)", req)
	isTruncated := true
	for isTruncated {
		// list all the objects
		res, err := d.oss.ListObjectsV2(ctx, req)
		if err != nil {
			return err
		}
		walkInfos := make([]storagedriver.FileInfoInternal, 0, len(res.Contents))
		for _, content := range res.Contents {
			if strings.HasSuffix(*content.Key, "/") { // directory
				walkInfos = append(walkInfos, storagedriver.FileInfoInternal{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir: true,
						Path:  strings.TrimRight(*content.Key, "/"),
					},
				})
			} else { // file object
				// last modification time
				walkInfos = append(walkInfos, storagedriver.FileInfoInternal{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir:   false,
						Size:    content.Size,
						ModTime: *content.LastModified,
						Path:    *content.Key,
					},
				})
			}
		}
		isTruncated = res.IsTruncated
		req.StartAfter = res.StartAfter
		// iterative
		for _, walkInfo := range walkInfos {
			// skip any results under the last skip directory
			if prevSkipDir != "" && strings.HasPrefix(walkInfo.Path(), prevSkipDir) {
				continue
			}

			err := f(walkInfo)
			*objectCount++

			if err != nil {
				if errors.Is(err, storagedriver.ErrSkipDir) {
					prevSkipDir = walkInfo.Path()
					continue
				}
				if errors.Is(err, storagedriver.ErrFilledBuffer) {
					isTruncated = false
					break
				}
				retError = err
				isTruncated = false
				break
			}
		}
	}
	if retError != nil {
		return retError
	}
	return nil
}
