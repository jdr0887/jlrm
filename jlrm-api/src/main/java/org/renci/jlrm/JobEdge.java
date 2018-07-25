package org.renci.jlrm;

import java.util.List;

import org.jgrapht.graph.DefaultEdge;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobEdge extends DefaultEdge {

    private static final long serialVersionUID = 3289410868411756519L;

    private List<String> inputLabelList;

    public JobEdge() {
        super();
    }

}
