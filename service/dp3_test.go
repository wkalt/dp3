package service_test

// Uncomment this test to run the service with the debugger.
// func TestDP3Service(t *testing.T) {
// 	ctx := context.Background()
// 	store, err := storage.NewDirectoryStore("../data")
// 	require.NoError(t, err)
// 	opts := []service.DP3Option{
// 		service.WithPort(8089),
// 		service.WithCacheSizeMegabytes(4096),
// 		service.WithStorageProvider(store),
// 		service.WithDatabasePath("../dp3.db"),
// 		service.WithWALDir("../waldir"),
// 	}
// 	svc := service.NewDP3Service()
// 	require.NoError(t, svc.Start(ctx, opts...))
// }
//
