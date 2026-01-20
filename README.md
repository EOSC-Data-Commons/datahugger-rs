# datahugger

Tool for fetching data from DOI or URL.

Support data repositories:

- [dataverse](https://dataverse.org/) (click [here](https://github.com/EOSC-Data-Commons/datahugger-rs/blob/master/dataverse-repo-list.md) to see all dataverse data repositories we support).
- [osf.io](https://osf.io/)

## Usage

### CLI

download the binary or brew, apt, curl..

To download all data from a database, run:

```console
datahugger download https://osf.io/3ua2c/
```

```console
⠉ Crawling osfstorage/final_model_results_combined/single_species_models_final/niche_additive/Procyon lotor_2025-05-09.rdata...
⠲ Crawling osfstorage/final_model_results_combined/single_species_models_final/niche_additive...
⠈ Crawling osfstorage/final_model_results_combined/single_species_models_final...
⠒ Crawling osfstorage/final_model_results_combined...
⠐ Crawling osfstorage...
o/f/c/event-cbg-intersection.csv                             [==>-------------------------------------] 47.20 MB/688.21 MB (   4.92 MB/s,  2m)
o/i/size_intra_ind_int_rs_mod_2025-06-20.rdata               [================>-----------------------] 44.01 MB/107.09 MB (   4.45 MB/s, 14s)
o/i/niche_intra_ind_int_rs_mod_2025-06-21.rdata              [==============>-------------------------] 38.84 MB/108.41 MB (   4.52 MB/s, 15s)
o/f/m/a/Corvus corax.pdf                                     [=========>------------------------------] 80.47 kB/329.85 kB ( 438.28 kB/s,  1s)
o/f/m/a/Lynx rufus.pdf                                       [----------------------------------------]      0 B/326.02 kB (       0 B/s,  0s)
o/f/m/a/Ursus arctos.pdf                                     [----------------------------------------]      0 B/319.05 kB (       0 B/s,  0s)
```

## License

All contributions must retain this attribution.

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

