import java.util.List;

public class TotalStatThread extends Thread {

    private List<TestSimpleThread> m_logicTheads;     // Потоки выполняющие логику

    public TotalStatThread(List<TestSimpleThread> logicThreads) {
        m_logicTheads = logicThreads;
    }

    public void run() {
        long beforeMillis = System.currentTimeMillis();

//        while (t)
//
//        long n = 0;
//        long currMillis = beforeMillis;
//        while (currMillis < beforeMillis + m_intervalMillis) {
//            n = n + execute(client, namespace);
//            currMillis = System.currentTimeMillis();
//        }
//
//        m_current_rows_per_sec = Math.round(n/((currMillis-beforeMillis)/1000));
//        m_total_secs += currMillis-beforeMillis;
//        m_total_rows += n;
//
//        System.out.println("[" + getName() + "][" + formatDate(currMillis) + "]: " + m_current_rows_per_sec + " per sec" );

    }

}
