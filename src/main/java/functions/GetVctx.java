package functions;

import htsjdk.tribble.readers.LineIteratorImpl;
import htsjdk.tribble.readers.LineReaderUtil;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GetVctx implements Function<String, VariantContext> {
    String VCFHeaderStrings;

    public GetVctx(String VCFHeaderStrings) {
        this.VCFHeaderStrings = VCFHeaderStrings;
    }

    @Override
    public VariantContext call(String line) throws Exception {
        final VCFCodec codec = new VCFCodec();
        codec.readActualHeader(new LineIteratorImpl(LineReaderUtil.fromStringReader(
                new StringReader(VCFHeaderStrings), LineReaderUtil.LineReaderOption.SYNCHRONOUS)));

        return codec.decode(line);
    }
}
