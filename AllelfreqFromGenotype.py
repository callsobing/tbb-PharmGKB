import sys
import os

if len(sys.argv) < 3:
    sys.exit('Usage: %s <input .ped file> <input .bim file>' % sys.argv[0])

if not os.path.exists(sys.argv[1]):
    sys.exit('ERROR: %s does not exist' % sys.argv[1])

if not os.path.exists(sys.argv[2]):
    sys.exit('ERROR: %s does not exist' % sys.argv[2])

# bim with ref format: 1	rs9701055	0	565433	T	C   T (the last one is the reference from the hg19) 
bim_file = open(sys.argv[2])
probe_list = []
for record in bim_file:
    record = record.rstrip()
    splitted_record = record.split("\t")
    if not len(splitted_record) > 5:
        continue
    chromo = splitted_record[0]
    variant_id = splitted_record[1]
    position = int(splitted_record[3])
    allele1 = splitted_record[4]
    allele2 = splitted_record[5]
    ref = splitted_record[6]
    ident = chromo + "\t" + variant_id + "\t" + str(position) + "\t" + allele1 + "\t" + allele2 + "\t" + ref
    probe_list.append(ident)
bim_file.close()

# input file format: pedigreeName IndividualID PapaID MamaID Sex affectionStatus genotypes...
ped_file = open(sys.argv[1])
first_run = True
samples = 0
for record in ped_file:
    samples += 1
    splitted_record = record.rstrip().split("\s")
    if first_run:
        genotype = [0] * ((len(splitted_record) - 5) / 2)
        first_run = False
    for i in range(6, len(splitted_record) - 1, 2):
        genotype_idx = i/2 - 3
        probe = probe_list[genotype_idx]
        ref = probe.split("\t")[5]
        if splitted_record[i] != ref:
            genotype[genotype_idx] += 1
        if splitted_record[i + 1] != ref:
            genotype[genotype_idx] += 1
ped_file.close()

genotype_idx = 0
for probes in probe_list:
    print(probes + "\t" + str(float(genotype[genotype_idx])/float(samples * 2)))
    genotype_idx += 1
