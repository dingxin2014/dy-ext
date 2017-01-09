import com.dooioo.extension.queue.DistributedQueue;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import sun.misc.Unsafe;
import sun.misc.VM;

/**
 * Created by dingxin on 17/1/4.
 */


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:test-spring.xml")
public class Test {

    public static class Inner {

        public
        static Unsafe getUnsafe(){
            return Unsafe.getUnsafe();
        }

    }

    @org.junit.Test
    public void test(){
        DistributedQueue<String> queue = new DistributedQueue<String>("eReceipt","test");
        queue.clear();
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                DistributedQueue<String> queue = new DistributedQueue<String>("eReceipt","test");
                for(int i=0;i<100;i++){
                    queue.add(String.valueOf(i));

                    System.out.println("add"+ String.valueOf(i));
                }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                DistributedQueue<String> queue = new DistributedQueue<String>("eReceipt","test");
                while(true) {
//                    for (String s : queue) {
//                        queue
//                        System.out.println("--> " + s);
//                    }
                    String s = queue.poll();
                    System.err.println("poll -> "+ String.valueOf(s));
                }
            }
        });

        t1.start();
        t2.start();

        while(true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
