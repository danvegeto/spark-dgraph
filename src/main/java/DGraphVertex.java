import java.util.List;

public interface DGraphVertex {

    String getUid();
    List<? extends DGraphVertex> getEdges(String edgeType);

}
