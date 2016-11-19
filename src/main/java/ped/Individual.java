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

import java.io.Serializable;
import java.util.Arrays;

public class Individual implements Serializable {
    /**
     * Family ID: An identifier for the family.
     * Sample(Individual) ID: A unique identifier for the sample (person).
     * Father's sample ID: The sample ID for the father of the current individual (or 0 if the father is not represented in the pedigree).
     * Mother's sample ID: The sample ID for the mother of the current individual (or 0 if the mother is not represented in the pedigree).
     * Gender: 1 for male, 2 for female, 0 if unknown.
     * Phenotype: 1 for unaffected, 2 for affected, 0 if unknown.
     * Parts: the collection of splitted Strings
     */
    private String familyId;
    private String individualId;
    private String fatherId;
    private String motherId;
    private String gender;
    private String phenotype;
    private String population;
    private String[] parts;

    public Individual(String familyId, String individualId, String fatherId,
                      String motherId, String gender, String phenotype, String population, String[] parts) {
        this.familyId = familyId;
        this.individualId = individualId;
        this.fatherId = fatherId;
        this.motherId = motherId;
        this.gender = gender;
        this.phenotype = phenotype;
        this.population = population;
        this.parts = parts;
    }

    public String getFamilyId() {
        return familyId;
    }

    public String getIndividualId() {
        return individualId;
    }

    public String getFatherId() {
        return fatherId;
    }

    public String getMotherId() {
        return motherId;
    }

    public String getGender() {
        return gender;
    }

    public String getPhenotype() {
        return phenotype;
    }

    public String getPopulation() {
        return population;
    }

    public String[] getParts() {
        return parts;
    }

    @Override
    public String toString() {
        return "Individual{" +
                "familyId='" + familyId + '\'' +
                ", individualId='" + individualId + '\'' +
                ", fatherId='" + fatherId + '\'' +
                ", motherId='" + motherId + '\'' +
                ", gender='" + gender + '\'' +
                ", phenotype='" + phenotype + '\'' +
                ", population='" + population + '\'' +
                ", parts=" + Arrays.toString(parts) +
                '}';
    }
}
