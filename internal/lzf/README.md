# LZF

Without code generation:

```
BenchmarkLZF_CompressFast12
BenchmarkLZF_CompressFast12-10         	    5346	    223166 ns/op	   16384 B/op	       1 allocs/op
BenchmarkLZF_CompressFast16
BenchmarkLZF_CompressFast16-10         	    5126	    231943 ns/op	  262144 B/op	       1 allocs/op
BenchmarkLZF_CompressFast20
BenchmarkLZF_CompressFast20-10         	    3523	    327466 ns/op	 4194307 B/op	       1 allocs/op
BenchmarkLZF_CompressVeryFast12
BenchmarkLZF_CompressVeryFast12-10     	   19915	     60816 ns/op	   16384 B/op	       1 allocs/op
BenchmarkLZF_CompressVeryFast16
BenchmarkLZF_CompressVeryFast16-10     	   18036	     67247 ns/op	  262146 B/op	       1 allocs/op
BenchmarkLZF_CompressVeryFast20
BenchmarkLZF_CompressVeryFast20-10     	    5953	    196715 ns/op	 4194314 B/op	       1 allocs/op
BenchmarkLZF_CompressUltraFast12
BenchmarkLZF_CompressUltraFast12-10    	   20250	     60594 ns/op	   16384 B/op	       1 allocs/op
BenchmarkLZF_CompressUltraFast16
BenchmarkLZF_CompressUltraFast16-10    	   17253	     72113 ns/op	  262146 B/op	       1 allocs/op
BenchmarkLZF_CompressUltraFast20
BenchmarkLZF_CompressUltraFast20-10    	    5990	    200742 ns/op	 4194314 B/op	       1 allocs/op
BenchmarkLZF_Decompress
BenchmarkLZF_Decompress-10             	  323216	      3691 ns/op	       0 B/op	       0 allocs/op
PASS
```

With code generation:

```
BenchmarkLZF_CompressFast12
BenchmarkLZF_CompressFast12-10         	    7846	    150880 ns/op	   16384 B/op	       1 allocs/op
BenchmarkLZF_CompressFast16
BenchmarkLZF_CompressFast16-10         	    7341	    159899 ns/op	  262144 B/op	       1 allocs/op
BenchmarkLZF_CompressFast20
BenchmarkLZF_CompressFast20-10         	    4570	    258674 ns/op	 4194308 B/op	       1 allocs/op
BenchmarkLZF_CompressVeryFast12
BenchmarkLZF_CompressVeryFast12-10     	   21448	     55825 ns/op	   16384 B/op	       1 allocs/op
BenchmarkLZF_CompressVeryFast16
BenchmarkLZF_CompressVeryFast16-10     	   19311	     62060 ns/op	  262146 B/op	       1 allocs/op
BenchmarkLZF_CompressVeryFast20
BenchmarkLZF_CompressVeryFast20-10     	    5863	    196414 ns/op	 4194319 B/op	       1 allocs/op
BenchmarkLZF_CompressUltraFast12
BenchmarkLZF_CompressUltraFast12-10    	   21543	     55353 ns/op	   16384 B/op	       1 allocs/op
BenchmarkLZF_CompressUltraFast16
BenchmarkLZF_CompressUltraFast16-10    	   18601	     64871 ns/op	  262146 B/op	       1 allocs/op
BenchmarkLZF_CompressUltraFast20
BenchmarkLZF_CompressUltraFast20-10    	    5620	    194928 ns/op	 4194317 B/op	       1 allocs/op
BenchmarkLZF_Decompress
BenchmarkLZF_Decompress-10             	  334724	      3591 ns/op	       0 B/op	       0 allocs/op
PASS
```
