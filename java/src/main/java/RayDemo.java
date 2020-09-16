import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;

public class RayDemo {

  public static int square(int x) {
    return x * x;
  }

  public static class Counter {

    private int value = 0;

    public void increment() {
      this.value += 1;
    }

    public int getValue() {
      return this.value;
    }
  }

  public static int callActorInWorker(ActorHandle<Counter> counter) {
    counter.task(Counter::increment).remote();
    return counter.task(Counter::getValue).remote().get();
  }

  public static void main(String[] args) {
    // 初始化Ray runtime。
    Ray.init();
    {
      // === Ray task 示例 ===

      List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
      // 通过`Ray.task(...).remote()`，我们可以把任意一个Java静态函数转化成Ray task，
      // 异步地远程执行这个函数。通过下面两行代码，我们并发地执行了5个Ray task。
      // `remote()`的返回值是一个`ObjectRef`对象，这个对象类似一个future，
      // 表示Task执行结果的引用。
      for (int i = 0; i < 5; i++) {
        objectRefs.add(Ray.task(RayDemo::square, i).remote());
      }
      // 实际的task执行结果存放在Ray的分布式object store里，
      // 我们可以通过`ray.get`接口，同步地获取这些数据。
      Assert.assertEquals(Ray.get(objectRefs), Arrays.asList(0, 1, 4, 9, 16));
    }
    {
      // === Ray actor 示例 ===

      // 通过`Ray.actor(...).remote`接口，我们可以基于任意一个Java class创建一个Ray actor.
      // 这个actor对象会运行在一个远程的Java进程中。
      ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();
      // 通过上面的接口，我们得到一个`ActorHandle`对象。
      // 通过`ActorHandle`，我们可以远程调用Actor的任意一个方法（actor task）。
      for (int i = 0; i < 5; i++) {
        counter.task(Counter::increment).remote();
      }
      // Actor task的返回值也是一个`ObjectRef`对象。
      // 同样地，我们通过`ray.get`获取实际的数据。
      ObjectRef<Integer> objectRef = counter.task(Counter::getValue).remote();
      Assert.assertEquals((int) objectRef.get(), 5);
    }
    {
      // === Ray object store 示例 ===

      ObjectRef<Integer> objectRef = Ray.put(1);
      Assert.assertEquals((int) objectRef.get(), 1);
    }
    {
      // === Ray workflow 示例 ===

      // 通过把一个task输出的`ObjectRef`传递给另一个task，我们定义了两个task的依赖关系。
      // Ray会等待第一个task执行结束之后，再开始执行第二个task。
      ObjectRef<Integer> objRef1 = Ray.task(RayDemo::square, 2).remote();
      ObjectRef<Integer> objRef2 = Ray.task(RayDemo::square, objRef1).remote();
      Assert.assertEquals((int) objRef2.get(), 16);

      // 我们也可以把一个`ActorHandle`传递给一个task，
      // 从而实现在多个远程worker中同时远程调用一个actor。
      ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();
      List<ObjectRef<Integer>> objRefs = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        objRefs.add(Ray.task(RayDemo::callActorInWorker, counter).remote());
      }
      List<Integer> results = Ray.get(objRefs);
      Collections.sort(results);
      Collections.sort(Ray.get(objRefs));
      Assert.assertEquals(results, Arrays.asList(1, 2, 3, 4, 5));
    }
  }
}
