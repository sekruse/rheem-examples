import org.qcri.rheem.api.DataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by zoi on 8/27/16.
 */
public class RunSGD {


    static String relativePath = "sgd/src/main/resources/adult.zeros";
    static int datasetSize  = 100827;
    static int features = 123;
    static boolean sparse = true;
    static boolean binary = true; //this applies only for SVM, to transform the 0 to -1 during the transform

    //these are for SGD/mini run to convergence, put < 1 for batch GD
    static int sampleSize = 6;
    static double accuracy = 0.001;
    static int max_iterations = 1000;


    public static void main (String... args) throws MalformedURLException {

        //Usage: <data_file> <#features> <sparse> <binary>
        if (args.length > 0) {
            relativePath = args[0];
            datasetSize = Integer.parseInt(args[1]);
            features = Integer.parseInt(args[2]);
            sparse = Boolean.parseBoolean(args[3]);
            binary = Boolean.parseBoolean(args[4]);
            max_iterations = Integer.parseInt(args[5]);
            accuracy = Double.parseDouble(args[6]);
            sampleSize = Integer.parseInt(args[7]);
        }
        else {
            System.out.println("Loading default values");
        }

        String file = new File(relativePath).getAbsoluteFile().toURI().toURL().toString();

        System.out.println("sparse:" + sparse);
        System.out.println("#iterations:" + max_iterations);
        System.out.println("accuracy:" + accuracy);

        new RunSGD().execute(file, features);
    }


    public void execute(String fileName, int features) {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<double[]> broadcast = Arrays.asList(new double[features]);
        final DataQuantaBuilder<?, double[]> weightsBuilder = javaPlanBuilder.loadCollection(broadcast).withName("init weights");

        final DataQuantaBuilder<?, SparseVector> transformBuilder = javaPlanBuilder
                .readTextFile(fileName).withName("source")
                .map(new Transform()).withName("transform");

        Collection<double[]> results  =
                weightsBuilder.doWhile(new LoopCondition(accuracy, max_iterations), w -> {

            DataQuantaBuilder<?, double[]> newWeightsDataset = transformBuilder
                    .sample(sampleSize)
                    .map(new ComputeLogisticGradient()).withBroadcast(w, "weights").withName("compute")
                    .reduce(new Reduce()).withName("reduce")
                    .map(new WeightsUpdate()).withBroadcast(w, "weights").withName("update");

            DataQuantaBuilder<?, Tuple2<Double, Double>> convergenceDataset = newWeightsDataset.map(new Converge()).withBroadcast(w, "weights");

            return new Tuple<>(newWeightsDataset.filter(w1 -> true), convergenceDataset);
        }).collect();

        System.out.println("Output weights:" + Arrays.toString(RheemCollections.getSingle(results)));

    }
}

class Transform implements FunctionDescriptor.SerializableFunction<String, SparseVector> {

    @Override
    public SparseVector apply(String line) {
        String[] pointStr = line.split(" ");
        int[] indices = new int[pointStr.length - 1];
        double[] values = new double[pointStr.length - 1];
        double label = Double.parseDouble(pointStr[0]);
        for (int i = 1; i < pointStr.length; i++) {
            if (pointStr[i].equals("")) {
                continue;
            }
            String kv[] = pointStr[i].split(":", 2);
            indices[i - 1] = Integer.parseInt(kv[0]);
            values[i - 1] = Double.parseDouble(kv[1]);
        }
        return new SparseVector(label, indices, values);
    }
}

class ComputeLogisticGradient implements FunctionDescriptor.ExtendedSerializableFunction<SparseVector, double[]> {

    double[] weights;

    @Override
    public double[] apply(SparseVector point) {
        double[] gradient = new double[weights.length + 1];
        double dot = 0;
        for (int i = 0; i < point.size(); i++)
            dot += weights[point.getIndices()[i] - 1] * point.getValues()[i];

        for (int i = 0; i < point.size(); i++)
            gradient[point.getIndices()[i]] = ((1 / (1 + Math.exp(-1 * dot))) - point.getLabel()) * point.getValues()[i];

        gradient[0] = 1; //counter required in the update

        return gradient;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
    }
}

class Reduce implements FunctionDescriptor.SerializableBinaryOperator<double[]> {

    @Override
    public double[] apply(double[] o, double[] o2) {
        double[] g1 = o;
        double[] g2 = o2;

        if (g2 == null) //samples came from one partition only
            return g1;

        if (g1 == null) //samples came from one partition only
            return g2;

        double[] sum = new double[g1.length];
        sum[0] = g1[0] + g2[0]; //count
        for (int i = 1; i < g1.length; i++)
            sum[i] = g1[i] + g2[i];

        return sum;
    }
}

class WeightsUpdate implements FunctionDescriptor.ExtendedSerializableFunction<double[], double[]> {

    double[] weights;
    int current_iteration;

    double stepSize = 1;
    double regulizer = 0;

    public WeightsUpdate () { }

    public WeightsUpdate (double stepSize, double regulizer) {
        this.stepSize = stepSize;
        this.regulizer = regulizer;
    }

    @Override
    public double[] apply(double[] input) {

        double count = input[0];
//        double alpha = (stepSize / iteration);
        double alpha = (stepSize / Math.sqrt(current_iteration+1));
        double[] newWeights = new double[weights.length];
        for (int j = 0; j < weights.length; j++) {
            newWeights[j] = (1 - alpha * regulizer) * weights[j] - alpha * (1.0 / count) * input[j + 1];
        }
        return newWeights;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.weights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        this.current_iteration = executionContext.getCurrentIteration();
    }
}

class Converge implements FunctionDescriptor.ExtendedSerializableFunction<double[], Tuple2<Double, Double>> {

    double[] previousWeights;
    int current_iteration;

    @Override
    public Tuple2<Double, Double> apply(double[] weights) {
        double normDiff = 0.0;
        double normWeights = 0.0;
        for (int j = 0; j < weights.length; j++) {
//            normDiff += Math.sqrt(Math.pow(Math.abs(weights[j] - input[j]), 2));
            normDiff += Math.abs(weights[j] - previousWeights[j]);
//            normWeights += Math.sqrt(Math.pow(Math.abs(input[j]), 2));
            normWeights += Math.abs(weights[j]);
        }
        return new Tuple2(normDiff, normWeights);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.previousWeights = (double[]) executionContext.getBroadcast("weights").iterator().next();
        this.current_iteration = executionContext.getCurrentIteration();
    }
}


class LoopCondition implements FunctionDescriptor.ExtendedSerializablePredicate<Collection<Tuple2<Double, Double>>> {

    public double accuracy;
    public int max_iterations;

    private int current_iteration;

    public LoopCondition(double accuracy, int max_iterations) {
        this.accuracy = accuracy;
        this.max_iterations = max_iterations;
    }

    @Override
    public boolean test(Collection<Tuple2<Double, Double>> collection) {
        Tuple2<Double, Double> input = RheemCollections.getSingle(collection);
        System.out.println("Running iteration: " + current_iteration);
        return (input.field0 < accuracy * Math.max(input.field1, 1.0) || current_iteration > max_iterations);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.current_iteration = executionContext.getCurrentIteration();
    }
}