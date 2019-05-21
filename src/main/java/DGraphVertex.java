import java.io.Serializable;
import java.util.List;

public interface DGraphVertex extends Serializable {
    public String getUid();
    public List<? extends DGraphVertex> getEdges(String edgeType);
}
