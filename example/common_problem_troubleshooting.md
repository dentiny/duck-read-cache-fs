### Common problem troubleshooting

#### Problem-1: program crashes due to SIGPIPE

`SIGPIPE` is notorious in network programming, which **could** be caused by improper handling to a closed socket. A common solution is directly ignore certain the signal and trigger TCP reconnection and rewrite.

cache httpfs provides such option (by default off),
```sql
D SET cache_httpfs_ignore_sigpipe=true;
```

It's by default off since it's a process-wise signal handling, rather than an extension-wise configuration.

Reference: https://blog.erratasec.com/2018/10/tcpip-sockets-and-sigpipe.html
