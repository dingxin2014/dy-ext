import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by dingxin on 17/1/4.
 */


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:test-spring.xml")
public class Test {


    @org.junit.Test
    public void test(){
        System.out.println("-->");
    }
}
