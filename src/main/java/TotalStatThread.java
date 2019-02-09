import java.util.List;

public class TotalStatThread extends Thread {

    private List<TestSimpleThread> m_logicTheads;     // Потоки выполняющие логику

    public TotalStatThread(List<TestSimpleThread> logicThreads) {
        m_logicTheads = logicThreads;
    }

    public void run() {
        while (getLogicThreadsAliveCnt() > 0) {
            try {
                sleep(1000);
            }
            catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
            long currMillis = System.currentTimeMillis();
            long m_current_rows_per_sec = 0;
            for(TestSimpleThread logicThread : m_logicTheads) {
                m_current_rows_per_sec += logicThread.getCurrentRowsPerSec();
            }
            System.out.println("[total][" + TestSimpleThread.formatDate(currMillis) + "]: " + m_current_rows_per_sec + " per sec" );
        }
    }

    private int getLogicThreadsAliveCnt() {
        int ret = 0;
        for(TestSimpleThread logicThread : m_logicTheads) {
            if (logicThread.isAlive()) {
                ret++;
            }
        }
        return ret;
    }
}
