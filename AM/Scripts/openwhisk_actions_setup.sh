# Create and run actions
wsk action create helloGo hello.go 
wsk action update helloGo --web true
wsk api create /helloGo /world get helloGo --response-type json


# ab -n 10000 http://node0:3234/api/23bc46b1-71f6-4ed5-8c54-816aa4f8c502/helloGo/world 