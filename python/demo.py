import ray


# 初始化Ray runtime
ray.init()

# === Ray task 示例 ===

# `square`是一个普通的Python函数，`@ray.remote`装饰器表示我们可以
# 把这个函数转化成Ray task.
@ray.remote
def square(x):
    return x * x


obj_refs = []
# `squire.remote` 会异步地远程执行`square`函数。
# 通过下面两行代码，我们并发地执行了5个Ray task。
# `square.remote`的返回值是一个`ObjectRef`对象，
# 表示Task执行结果的引用。
for i in range(5):
    obj_refs.append(square.remote(i))

# 实际的task执行结果存放在Ray的分布式object store里，
# 我们可以通过`ray.get`接口，同步地获取这些数据。
assert ray.get(obj_refs) == [0, 1, 4, 9, 16]

# === Ray actor 示例 ===

# `Counter`是一个普通的Python类，`@ray.remote`装饰器表示我们可以
# 把这个类转化成Ray actor.
@ray.remote
class Counter(object):

    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1

    def get_value(self):
        return self.value


# `Counter.remote`会基于`Counter`类创建一个actor对象，
# 这个actor对象会运行在一个远程的Python进程中。
counter = Counter.remote()

# `Counter.remote`的返回值是一个`ActorHandle`对象。
# 通过`ActorHandle`，我们可以远程调用Actor的任意一个方法（actor task）。
[counter.increment.remote() for _ in range(5)]

# Actor task的返回值也是一个`ObjectRef`对象。
# 同样地，我们通过`ray.get`获取实际的数据。
assert ray.get(counter.get_value.remote()) == 5

# === Ray object store 示例 ===

# 显式地把一个对象放入object store。
obj_ref = ray.put(1)
assert ray.get(obj_ref) == 1

# === Ray workflow 示例 ===

# 通过把一个task输出的`ObjectRef`传递给另一个task，
# 我们定义了两个task的依赖关系。
# Ray会等待第一个task执行结束之后，再开始执行第二个task。
obj1 = square.remote(2)
obj2 = square.remote(obj1)
assert ray.get(obj2) == 16


# 我们也可以把一个`ActorHandle`传递给一个task，
# 从而实现在多个远程worker中同时远程调用一个actor。
@ray.remote
def call_actor_in_worker(counter):
    counter.increment.remote()


counter = Counter.remote()
# 创建5个task，同时调用counter actor的increment方法，
# 并等待这五个task执行完。
ray.get([call_actor_in_worker.remote(counter) for _ in range(5)])
assert ray.get(counter.get_value.remote()) == 5
