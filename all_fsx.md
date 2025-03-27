ubuntu@ip-26-0-163-127:/fsx/mathieu_morlon/psync_size_per_user$ export GOMEMLIMIT=10GiB GOGC=off CRAWL_WORKERS=1000
ubuntu@ip-26-0-163-127:/fsx/mathieu_morlon/psync_size_per_user$ time sudo -E ./psync /fsx
intiating 1000 workers
Alloc = 559 MiB	TotalAlloc = 559 MiB	Sys = 681 MiB	NumGC = 0
Alloc = 1095 MiB	TotalAlloc = 1095 MiB	Sys = 1239 MiB	NumGC = 0
Alloc = 1606 MiB	TotalAlloc = 1606 MiB	Sys = 1774 MiB	NumGC = 0
Alloc = 4986 MiB	TotalAlloc = 60214 MiB	Sys = 9876 MiB	NumGC = 6
Alloc = 5051 MiB	TotalAlloc = 60279 MiB	Sys = 9876 MiB	NumGC = 6
                      lewis (lewis@huggingface.co) =>  9.9T
      simon_alibert (simon.alibert@huggingface.co) =>  469.2G
benjamin_burtenshaw (benjamin.burtenshaw@huggingface.co) =>  490.6G
        elie_bakouch (elie.bakouch@huggingface.co) =>  16.6T
                                            150014 =>  181.7G
                      pablo (pablo@huggingface.co) =>  2.5T
    mathieu_morlon (mathieu.morlon@huggingface.co) =>  30.8G
                      mohit (mohit@huggingface.co) =>  860.6G
eustache_lebihan (eustache.lebihan@huggingface.co) =>  2.1T
              guilherme (guilherme@huggingface.co) =>  5.8T
                  aymeric (aymeric@huggingface.co) =>  9.2G
michel_aractingi (michel.aractingi@huggingface.co) =>  2.0T
                                            150008 =>  79.8G
                  leandro (leandro@huggingface.co) =>  1.1T
                      anton (anton@huggingface.co) =>  2.8T
        daniel_dekok (daniel.dekok@huggingface.co) =>   818
        miquel_farre (miquel.farre@huggingface.co) =>  3.6T
                                   ubuntu (Ubuntu) =>  691.4G
            tom_aarsen (tom.aarsen@huggingface.co) =>  2.0T
                                            150190 =>  98.0G
      clement_romac (clement.romac@huggingface.co) =>  14.6G
                      pedro (pedro@huggingface.co) =>  4.9T
                      sayak (sayak@huggingface.co) =>  1.6T
                      ilyas (ilyas@huggingface.co) =>  15.7M
                                            150150 =>  181.9G
                                orr_zohar (<mail>) =>  12.0T
agustin_piqueres (agustin.piqueres@huggingface.co) =>  1.9T
andres_marafioti (andres.marafioti@huggingface.co) =>  1.5T
    sasha_luccioni (sasha.luccioni@huggingface.co) =>  2.0T
                    pepijn (pepijn@huggingface.co) =>  7.7G
                    arthur (arthur@huggingface.co) =>  1.9T
    mustafa_shukor (mustafa.shukor@huggingface.co) =>  1.9T
                  craffel (craffel@huggingface.co) =>  558.1G
    hynek_kydlicek (hynek.kydlicek@huggingface.co) =>  8.9T
                                       root (root) =>  195.7G
          haojun_zhao (haojun.zhao@huggingface.co) =>  2.1T
            clementine (clementine@huggingface.co) =>  1.4T
                                            150119 =>  721.4G
        cyril_vallez (cyril.vallez@huggingface.co) =>  733.8G
alvaro_bartolome (alvaro.bartolome@huggingface.co) =>  41.9G
                    kashif (kashif@huggingface.co) =>  1.8T
                        adil (adil@huggingface.co) =>  18.9G
        steven_palma (steven.palma@huggingface.co) =>  7.7G
        hugo_larcher (hugo.larcher@huggingface.co) =>  544.9G
    baptiste_colle (baptiste.colle@huggingface.co) =>  6.5M
                  vaibhav (vaibhav@huggingface.co) =>  1.9T
alina_lozovskaia (alina.lozovskaia@huggingface.co) =>  2.4T
  dana_aubakirova (dana.aubakirova@huggingface.co) =>  578.0G
quentin_gallouedec (quentin.gallouedec@huggingface.co) =>  1.8T
      shirin_yamani (shirin.yamani@huggingface.co) =>  56.8G
                    loubna (loubna@huggingface.co) =>  37.5T
                                            150010 =>  421.7G
  matej_sirovatka (matej.sirovatka@huggingface.co) =>  42.7G
        nathan_habib (nathan.habib@huggingface.co) =>  3.4T
                mfuntowicz (morgan@huggingface.co) =>  16.2G
           muellerzr (zach.mueller@huggingface.co) =>  109.5G
          phuc_nguyen (phuc.nguyen@huggingface.co) =>  14.7T
          remi_cadene (remi.cadene@huggingface.co) =>  3.2T
  matthew_douglas (matthew.douglas@huggingface.co) =>  674.7G
                                derek_liu (<mail>) =>  218.1G
                      aryan (aryan@huggingface.co) =>  281.1G
                     thomwolf (thomwolf@gmail.com) =>  137.9G
          yoni_gozlan (yoni.gozlan@huggingface.co) =>  151.6G
                        marc (marc@huggingface.co) =>  7.3G
          andrew_reed (andrew.reed@huggingface.co) =>  587.6G
      ferdinand_mom (ferdinand.mom@huggingface.co) =>  2.8T
                    edward (edward@huggingface.co) =>  15.3T
jason_stillerman (jason.stillerman@huggingface.co) =>  15.9T
                        ivar (ivar@huggingface.co) =>  26.7G
                                            150068 =>  40.2G
                 nicolas (patry.nicolas@gmail.com) =>  474.3G
                                            150066 =>  752.9G
                nouamane (nouamane@huggingface.co) =>  2.0T
                      titus (titus@huggingface.co) =>  28.9G
                                            150136 =>  1.7T
mohamed_mekkouri (mohamed.mekkouri@huggingface.co) =>  2.3T
                                            150041 =>  15.0G
                      dylan (dylan@huggingface.co) =>  1.7G

real	6m40.138s
user	3m11.512s
sys	131m55.779s
