import java.util.List;

public class TotalStatThread extends Thread {

    private List<TestSimpleThread> m_logicTheads;     // Потоки выполняющие логику

    public TotalStatThread(List<TestSimpleThread> logicThreads) {
        m_logicTheads = logicThreads;
    }

}
