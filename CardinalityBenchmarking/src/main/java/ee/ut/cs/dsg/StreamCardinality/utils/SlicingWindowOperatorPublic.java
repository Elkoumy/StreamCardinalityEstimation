package ee.ut.cs.dsg.StreamCardinality.utils;


import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.WindowOperator;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.slicing.SliceManager;
import de.tub.dima.scotty.slicing.StreamSlicer;
import de.tub.dima.scotty.slicing.WindowManager;
import de.tub.dima.scotty.slicing.aggregationstore.AggregationStore;
import de.tub.dima.scotty.slicing.aggregationstore.LazyAggregateStore;
import de.tub.dima.scotty.slicing.slice.SliceFactory;
import de.tub.dima.scotty.state.StateFactory;

import java.util.List;

public class SlicingWindowOperatorPublic<InputType> implements WindowOperator<InputType> {
    private final StateFactory stateFactory;

    public WindowManager getWindowManager() {
        return windowManager;
    }

    public SliceManager<InputType> getSliceManager() {
        return sliceManager;
    }

    private final WindowManager windowManager;
    private final SliceFactory<Integer, Integer> sliceFactory;
    private final SliceManager<InputType> sliceManager;
    private final StreamSlicer slicer;

    public SlicingWindowOperatorPublic(StateFactory stateFactory) {
        AggregationStore<InputType> aggregationStore = new LazyAggregateStore();
        this.stateFactory = stateFactory;
        this.windowManager = new WindowManager(stateFactory, aggregationStore);
        this.sliceFactory = new SliceFactory(this.windowManager, stateFactory);
        this.sliceManager = new SliceManager(this.sliceFactory, aggregationStore, this.windowManager);
        this.slicer = new StreamSlicer(this.sliceManager, this.windowManager);
    }

    public void processElement(InputType element, long ts) {
        this.slicer.determineSlices(ts);
        this.sliceManager.processElement(element, ts);
    }

    public List<AggregateWindow> processWatermark(long watermarkTs) {
        return this.windowManager.processWatermark(watermarkTs);
    }

    public void addWindowAssigner(Window window) {
        this.windowManager.addWindowAssigner(window);
    }

    public <OutputType> void addAggregation(AggregateFunction<InputType, ?, OutputType> windowFunction) {
        this.windowManager.addAggregation(windowFunction);
    }

    public <Agg, OutputType> void addWindowFunction(AggregateFunction<InputType, Agg, OutputType> windowFunction) {
        this.windowManager.addAggregation(windowFunction);
    }
}

