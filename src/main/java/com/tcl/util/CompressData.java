package com.tcl.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressData implements Runnable {
    private static final Configuration conf = new Configuration();
    private static final ArrayList<Path> files = new ArrayList();
    private Path file = null;

    public CompressData() {
    }

    public CompressData(String file) {
        this.file = new Path(file);
    }

    public CompressData(Path p) {
        this.file = p;
    }

    class IncludePathFilter implements PathFilter {
        private final String regex;

        public IncludePathFilter(String regex) {
            this.regex = regex;
        }

        public boolean accept(Path path) {
            return path.toString().matches(this.regex);
        }
    }

    public static String getSystemDate() {
        Date today = new Date();
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        return f.format(today);
    }

    public static boolean needCompress(FileStatus fileStatus) {
        if (fileStatus.isDir()) {
            return false;
        }
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(fileStatus.getPath());
        if (codec != null) {
            return false;
        }
        return true;
    }

    public static void getFiles(Path rootPath, String day) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fss = fs.listStatus(rootPath);
        for (FileStatus fileStatus : fss) {
            if (fileStatus.isDir()) {
                getFiles(fileStatus.getPath(), day);
            } else if (needCompress(fileStatus)) {
                Path p = fileStatus.getPath();
                String today = getSystemDate();
                if (!p.toString().contains(today)) {
                    if (((day != null) && (p.toString().contains(day)) && (!p.toString().contains("_SUCCESS"))) || ((day == null) && (!p.toString().contains("_SUCCESS")))) {
                        files.add(fileStatus.getPath());
                    }
                } else {
                    System.out.println(String.format("file path [%s] contains datestamp of system today, it is not safe to compress this file, so skipped!", new Object[] { p.toString() }));
                }
            }
        }
    }

    public static void compress(Path p, String codecClassname) throws ClassNotFoundException, IOException {
        FileSystem fs = FileSystem.get(conf);
        if (!fs.getFileStatus(p).isDir()) {
            Class<?> codecClass = Class.forName(codecClassname);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            Path output = p.suffix(codec.getDefaultExtension());
            BufferedInputStream is = new BufferedInputStream(fs.open(p));
            FSDataOutputStream os = fs.create(output);
            CompressionOutputStream out = codec.createOutputStream(os);
            IOUtils.copyBytes(is, out, 4096, true);
            fs.delete(p, false);
            System.out.println(String.format("[%s] compressed using [%s]", new Object[] { p, codecClassname }));
        }
    }

    public void startWithThreads(String dir, int threads, String day) throws InterruptedException, IOException {
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        getFiles(new Path(dir), day);
        for (Path p : files) {
            exec.execute(new CompressData(p));
        }
        exec.shutdown();
        exec.awaitTermination(12L, TimeUnit.HOURS);
    }

    public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, InterruptedException {
        Options options = new Options();
        options.addOption("dir", true, "Specify the dir contains files need to be compressed");
        options.addOption("threads", true, "threads num");
        options.addOption("day", true, "which day is today");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        String dir = cmd.getOptionValue("dir", "/user/hive/warehouse/rawdata/");
        int threads = Integer.parseInt(cmd.getOptionValue("threads", "10"));
        String day = cmd.getOptionValue("day", null);

        CompressData compressData = new CompressData();
        compressData.startWithThreads(dir, threads, day);
    }

    public void run() {
        try {
            compress(this.file, "org.apache.hadoop.io.compress.SnappyCodec");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
