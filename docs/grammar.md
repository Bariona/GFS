Go relevant grammar:

1. interface{}: can accept any type
2. RPC rule: 
   方法只接受两个可序列化参数, 其中第二个参数是指针类型，并且返回一个 error 类型，同时必须是公开的方法。

3. defer在return之后, defer 栈