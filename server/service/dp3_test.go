package service_test

// Uncomment this test to run the service with the debugger.
// func TestDP3Service(t *testing.T) {
// 	ctx := context.Background()
//
// 	store, err := storage.NewDirectoryStore("../../data")
// 	require.NoError(t, err)
//
// 	// Uncomment and fill in to use with S3
// 	//serverS3Endpoint := ""
// 	//serverS3AccessKey := ""
// 	//serverS3SecretKey := ""
// 	//serverS3Bucket := ""
// 	//serverS3Region := ""
// 	//serverS3UseTLS := true
// 	//serverS3MultipartPartSizeMB := 5
// 	//mc, err := minio.New(serverS3Endpoint, &minio.Options{
// 	//	Creds:  credentials.NewStaticV4(serverS3AccessKey, serverS3SecretKey, ""),
// 	//	Secure: serverS3UseTLS,
// 	//	Region: serverS3Region,
// 	//})
// 	//require.NoError(t, err)
//
// 	//store := storage.NewS3Store(mc, serverS3Bucket, serverS3MultipartPartSizeMB*1024*1024)
//
// 	opts := []service.DP3Option{
// 		service.WithPort(8089),
// 		service.WithCacheSizeMegabytes(4096),
// 		service.WithStorageProvider(store),
// 		service.WithDatabasePath("../../dp3.db"),
// 		service.WithWALDir("../../waldir"),
// 	}
// 	svc := service.NewDP3Service()
// 	require.NoError(t, svc.Start(ctx, opts...))
// }
