package c4lab;

import htsjdk.tribble.readers.LineIteratorImpl;
import htsjdk.tribble.readers.LineReaderUtil;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import functions.GetVctx;

import java.io.*;
import java.util.*;

public class vcfProcessor implements Serializable {
    public static void main(String[] args) throws IOException {
        final String vcfPath = args[0];
        List<String> rsidList = Arrays.asList("rs1042713","rs1045642","rs1050828","rs1057910","rs113993959","rs113993960","rs116855232","rs121434568","rs121434569","rs121908755","rs121908757","rs121909005","rs121909041","rs12248560","rs12979860","rs145489027","rs1695","rs17244841","rs17708472","rs1799752","rs1799853","rs1799978","rs1800497","rs1800566","rs1801131","rs1801133","rs193922525","rs1954787","rs2032582","rs2108622","rs2228001","rs2279343","rs2279345","rs2297595","rs2359612","rs264631","rs264651","rs267606617","rs267606723","rs2740574","rs28371686","rs28399499","rs28399504","rs2884737","rs3745274","rs3892097","rs3918290","rs4148323","rs4149015","rs4149056","rs4244285","rs4680","rs4917639","rs4986893","rs55886062","rs56165452","rs6025","rs61742245","rs67376798","rs7294","rs7412","rs74503330","rs75039782","rs75527207","rs77010898","rs776746","rs7900194","rs80282562","rs8050894","rs8099917","rs8175347","rs887829","rs9923231","rs9934438");
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile(vcfPath);

        final String VCFHeaderStrings = file
                .filter(line -> line.startsWith("#"))
                .reduce((s1, s2) -> s1 + "\n" + s2);

        JavaRDD<String> lines = file.filter(line -> !line.startsWith("#"));

        List<String> sampleHeaderRaw =
                Arrays.asList(file.filter(line -> line.startsWith("#CHROM")).first().split("\\t"));
        List<String> sampleHeader = new ArrayList<>(sampleHeaderRaw.subList(9, sampleHeaderRaw.size()));

        JavaRDD<VariantContext> vctx = lines
                .map(new GetVctx(VCFHeaderStrings));

        List<String> sampleIdList = new ArrayList<>();
        for(int k = 0; k <= sampleHeader.size()-1; k++) {
            sampleIdList.add(sampleHeader.get(k));
        }

        JavaPairRDD<String, String> aa = vctx.flatMapToPair(new PairFlatMapFunction<VariantContext, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(VariantContext variantContext) throws Exception {
                List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
                String [] variantIds = variantContext.getID().split(",");
                for(String variantId: variantIds) {
                    if (rsidList.contains(variantId)) {
                        if (variantContext.getAlleles().size() > 2) {
                            output.add(new Tuple2<String, String>(variantContext.getID(), "got more than one alternative alleles"));
                        }
                        float chromosomeCount = 0;
                        float sampleChromCount = 0;
                        List<String> sampleIds = variantContext.getSampleNamesOrderedByName();
                        for (String sampleid : sampleIds) {
                            chromosomeCount += variantContext.getGenotype(sampleid).countAllele(variantContext.getAlternateAllele(0));
                            sampleChromCount += 2;
                        }
                        Float allelFreq = chromosomeCount / sampleChromCount;
                        output.add(new Tuple2<String, String>(variantContext.getID(), allelFreq.toString()));
                    }
                }
                return output.iterator();
            }
        });

//
//        JavaPairRDD<String, String> aa = vctx.mapToPair(new PairFunction<VariantContext, String, String>() {
//            @Override
//            public Tuple2<String, String> call(VariantContext variantContext) throws Exception {
//                if(rsidList.contains(variantContext.getID())) {
//                    if(variantContext.getAlleles().size() > 2){
//                        return new Tuple2<String, String>(variantContext.getID(), "got more than one alternative alleles");
//                    }
//                    float chromosomeCount = 0;
//                    float sampleChromCount = 0;
//                    List<String> sampleIds = variantContext.getSampleNamesOrderedByName();
//                    for(String sampleid: sampleIds){
//                        chromosomeCount += variantContext.getGenotype(sampleid).countAllele(variantContext.getAlternateAllele(0));
//                        sampleChromCount += 2;
//                    }
//                    Float allelFreq = chromosomeCount/sampleChromCount;
//                    return new Tuple2<String, String>(variantContext.getID(), allelFreq.toString());
//                }
//                return new Tuple2<String, String>("none", "");
//            }
//        }).filter(x -> x._1.equals("none"));


        aa.saveAsTextFile("file:///root/pgkb/pgkbin1kg/");
    }
}
