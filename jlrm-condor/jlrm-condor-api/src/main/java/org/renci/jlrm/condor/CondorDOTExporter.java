package org.renci.jlrm.condor;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Properties;

import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.ext.ComponentAttributeProvider;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.VertexNameProvider;

/**
 * Nearly identical to DOTExporter except I allow for properties to be set...see dotProperties
 * 
 * @author jdr0887
 * 
 * @param <V>
 * @param <E>
 */
public class CondorDOTExporter<V, E> {

    private VertexNameProvider<V> vertexIDProvider;

    private VertexNameProvider<V> vertexLabelProvider;

    private EdgeNameProvider<E> edgeLabelProvider;

    private ComponentAttributeProvider<V> vertexAttributeProvider;

    private ComponentAttributeProvider<E> edgeAttributeProvider;

    private Properties dotProperties;

    public CondorDOTExporter() {
        this(new IntegerNameProvider<V>(), null, null, null);
    }

    public CondorDOTExporter(VertexNameProvider<V> vertexIDProvider, VertexNameProvider<V> vertexLabelProvider,
            EdgeNameProvider<E> edgeLabelProvider) {
        this(vertexIDProvider, vertexLabelProvider, edgeLabelProvider, null, null, null);
    }

    public CondorDOTExporter(VertexNameProvider<V> vertexIDProvider, VertexNameProvider<V> vertexLabelProvider,
            EdgeNameProvider<E> edgeLabelProvider, Properties dotProperties) {
        this(vertexIDProvider, vertexLabelProvider, edgeLabelProvider, null, null, dotProperties);
    }

    public CondorDOTExporter(VertexNameProvider<V> vertexIDProvider, VertexNameProvider<V> vertexLabelProvider,
            EdgeNameProvider<E> edgeLabelProvider, ComponentAttributeProvider<V> vertexAttributeProvider,
            ComponentAttributeProvider<E> edgeAttributeProvider, Properties dotProperties) {
        super();
        this.vertexIDProvider = vertexIDProvider;
        this.vertexLabelProvider = vertexLabelProvider;
        this.edgeLabelProvider = edgeLabelProvider;
        this.vertexAttributeProvider = vertexAttributeProvider;
        this.edgeAttributeProvider = edgeAttributeProvider;
        this.dotProperties = dotProperties;
    }

    public void export(Writer writer, Graph<V, E> g) {
        PrintWriter out = new PrintWriter(writer);
        String indent = "  ";
        String connector;

        if (g instanceof DirectedGraph<?, ?>) {
            out.println("digraph G {");
            connector = " -> ";
        } else {
            out.println("graph G {");
            connector = " -- ";
        }

        if (this.dotProperties != null && !this.dotProperties.isEmpty()) {
            for (Object key : this.dotProperties.keySet()) {
                Object value = this.dotProperties.get(key);
                out.println(String.format("%s=%s", key.toString(), value.toString()));
            }
        }

        for (V v : g.vertexSet()) {
            out.print(indent + getVertexID(v));

            String labelName = null;
            if (vertexLabelProvider != null) {
                labelName = vertexLabelProvider.getVertexName(v);
            }
            Map<String, String> attributes = null;
            if (vertexAttributeProvider != null) {
                attributes = vertexAttributeProvider.getComponentAttributes(v);
            }
            renderAttributes(out, labelName, attributes);

            out.println(";");
        }

        for (E e : g.edgeSet()) {
            String source = getVertexID(g.getEdgeSource(e));
            String target = getVertexID(g.getEdgeTarget(e));

            out.print(indent + source + connector + target);

            String labelName = null;
            if (edgeLabelProvider != null) {
                labelName = edgeLabelProvider.getEdgeName(e);
            }
            Map<String, String> attributes = null;
            if (edgeAttributeProvider != null) {
                attributes = edgeAttributeProvider.getComponentAttributes(e);
            }
            renderAttributes(out, labelName, attributes);

            out.println(";");
        }

        out.println("}");

        out.flush();
    }

    protected void renderAttributes(PrintWriter out, String labelName, Map<String, String> attributes) {
        if ((labelName == null) && (attributes == null)) {
            return;
        }
        out.print(" [ ");
        if ((labelName == null) && (attributes != null)) {
            labelName = attributes.get("label");
        }
        if (labelName != null) {
            out.print("label=\"" + labelName + "\" ");
        }
        if (attributes != null) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                String name = entry.getKey();
                if (name.equals("label")) {
                    // already handled by special case above
                    continue;
                }
                out.print(name + "=\"" + entry.getValue() + "\" ");
            }
        }
        out.print("]");
    }

    protected String getVertexID(V v) {
        String idCandidate = vertexIDProvider.getVertexName(v);

        // now test that this is a valid ID
        boolean isAlphaDig = idCandidate.matches("[a-zA-Z]+([\\w_]*)?");
        boolean isDoubleQuoted = idCandidate.matches("\".*\"");
        boolean isDotNumber = idCandidate.matches("[-]?([.][0-9]+|[0-9]+([.][0-9]*)?)");
        boolean isHTML = idCandidate.matches("<.*>");

        if (isAlphaDig || isDotNumber || isDoubleQuoted || isHTML) {
            return idCandidate;
        }

        throw new RuntimeException("Generated id '" + idCandidate + "'for vertex '" + v
                + "' is not valid with respect to the .dot language");
    }
}