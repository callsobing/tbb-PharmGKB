/**
 * Copyright (C) 2015, Anome Incorporated. All Rights Reserved.
 * This program is an unpublished copyrighted work which is proprietary to
 * Anome Incorporated and contains confidential information that is not to
 * be reproduced or disclosed to any other person or entity without prior
 * written consent from Anome, Inc. in each and every instance.
 * <p>
 * Unauthorized reproduction of this program as well as unauthorized
 * preparation of derivative works based upon the program or distribution of
 * copies by sale, rental, lease or lending are violations of federal copyright
 * laws and state trade secret laws, punishable by civil and criminal penalties.
 */
package ped;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Pedigree implements Iterable<Individual>, Serializable {
    private static final String UNKNOWN = "0";
    private static final String UNAFFECTED = "1";
    private static final String AFFECTED = "2";
    private List<Individual> individuals;

    public Pedigree() {
        individuals = Collections.emptyList();
    }

    public Pedigree(Path filePath) throws IOException {
        individuals = new ArrayList<>();

        FileSystem fs = FileSystem.get(new Configuration());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
            String line;

            while ((line = br.readLine()) != null) {    // content start here
                String[] cols = line.split("\\s+");
                Individual indiv = new Individual(cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols);
                individuals.add(indiv);
            }
        }
    }

    public List<Individual> getIndividuals() {
        return individuals;
    }

    public List<String> getIndividualIds() {
        return individuals.stream().map(Individual::getIndividualId).collect(Collectors.toList());
    }

    public List<Individual> getAffected() {
        return this.individuals.stream()
                .filter(indiv -> indiv.getPhenotype().equals(AFFECTED))
                .collect(Collectors.toList());
    }

    public List<Individual> getChildswithPapaAndMama() {
        return this.individuals.stream()
                .filter(indiv -> !indiv.getFatherId().equals("0"))
                .filter(indiv -> !indiv.getMotherId().equals("0"))
                .collect(Collectors.toList());
    }

    public Set<String> getAffecteID() {
        return this.individuals.stream()
                .filter(indiv -> indiv.getPhenotype().equals(AFFECTED))
                .map(Individual::getIndividualId)
                .collect(Collectors.toSet());
    }

    public List<Individual> getUnaffected() {
        return this.individuals.stream()
                .filter(indiv -> indiv.getPhenotype().equals(UNAFFECTED))
                .collect(Collectors.toList());
    }

    public Set<String> getUnaffecteID() {
        return this.individuals.stream()
                .filter(indiv -> indiv.getPhenotype().equals(UNAFFECTED))
                .map(Individual::getIndividualId)
                .collect(Collectors.toSet());
    }

    public List<Individual> getUnknown() {
        return this.individuals.stream()
                .filter(indiv -> indiv.getPhenotype().equals(UNKNOWN))
                .collect(Collectors.toList());
    }

    public List<Individual> filterBySpecificField(String query, int col) {
        return this.individuals.stream()
                .filter(indiv -> indiv.getParts()[col].toLowerCase().equals(query))
                .collect(Collectors.toList());
    }

    public boolean hasIndividual() {
        return this.individuals.size() > 0;
    }

    @Override
    public Iterator<Individual> iterator() {
        return individuals.iterator();
    }

    @Override
    public void forEach(Consumer<? super Individual> action) {
        individuals.forEach(action);
    }

    @Override
    public Spliterator<Individual> spliterator() {
        return individuals.spliterator();
    }
}
