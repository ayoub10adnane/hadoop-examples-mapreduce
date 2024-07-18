package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districts", DistrictsContainingTrees.class,
                    "A map/reduce program that lists the distinct districts containing trees.");
            programDriver.addClass("species", ShowAllSpecies.class,
                    "A map/reduce program that lists all species of trees.");
            programDriver.addClass("treesbykind", NumberOfTreesByKind.class,
                    "A map/reduce program that counts the number of trees by kind.");
            programDriver.addClass("maxheight", MaxHeightPerKind.class,
                    "A map/reduce program that finds the maximum height of each kind of tree.");
            programDriver.addClass("sortheight", SortHeight.class,
                    "A map/reduce program that sorts trees by height from smallest to largest.");
            programDriver.addClass("oldesttree", DistrictContainingOldestTree.class,
                    "A map/reduce program that finds the district containing the oldest tree.");
            programDriver.addClass("mosttrees", DistrictWithMostTreesPhase1.class,
                    "A map/reduce program that finds the district containing the most trees.");
            programDriver.addClass("clear", DistrictWithMostTreesPhase2.class,
                    "A map/reduce program that finds the district containing the most trees (Phase 2).");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
