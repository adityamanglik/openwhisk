# Create and run actions
wsk action create GoLL LL1000128M.go --web true --concurrency 1
wsk action update GoLL LL1000128M.go --annotation exec.containers.max 1 --annotation exec.concurrent.max 1
wsk action update GoLL LL1000128M.go --annotation concurrency 1 --timeout 120000
# wsk action update GoLL LL1000128M.go --memory 128
wsk api create /GoLL get GoLL --response-type json


# ab -n 10000 http://node0:3234/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/helloGo/world 