import org.junit.Test;

/**
 * @author gy
 * @version 1.0
 * @description TODO
 * @date 2022/12/27 10:22
 */
public class ThreadTest {

    @Test
    public void test(){
        Thread thread = new Thread();
        thread.run();
        thread.start();
    }
}
