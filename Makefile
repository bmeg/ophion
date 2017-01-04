
proto : 
	protoc -I . -I schema/googleapis  --python_out=. schema/query.proto