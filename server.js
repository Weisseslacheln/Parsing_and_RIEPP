import * as parsing from "./parsing";

import {
  MergeOrg,
  MergeOrg2,
  // AllOrgs,
  AllAuthors,
  ids_to_rnf_orgs,
  ids_to_rnf_auf,
  rinc_authors_to_rnf,
  idscience_authors_to_rnf,
  allOrganizationsIdscience_with_authors,
  all_base_letters_ё_й,
} from "./lookup";

import * as collect_info from "./lookup/collect_info";
import * as scopus_affs from "./lookup/scopus_affs";
import * as crossref from "./crossref";

import { measure } from "./measure.js";

// parsing.Parsing("test", "RNF");
// parsing.GetProject("test", "RNF", "RNF_project");

// parsing.GetProject_en("main", "RNF", "RNF_project_en_new");
// parsing.GetOrgs("test", "allOrgs", "allOrgs_Idscience_full2");
// parsing.GetAuthors("test", "allAuthors", "allAuthors_Idscience2");
// parsing.GetALLOrgs("test", "allOrgsIdscience");
//скачал все по годам так что там дубли
//убрал в функции

// MergeOrg("test", "RNF_project", "1.basic", "basic_merge");
// MergeOrg2("test", "RNF_project", "1.basic", "basic_merge");
// AllOrgs("test", "RNF_project", "allOrgs");
// AllAuthors("test", "RNF_project", "allAuthors");

// ids_to_rnf_orgs("test", "ids-to-open(orgs)", "allOrgs", "res_orgs_ne[]");
// посмотерть что там с теми что не нашлись

// ids_to_rnf_auf("test", "allAuthorsIdscience", "allAuthors", "res_auf");
// rinc_authors_to_rnf(
//   "test",
//   "rinc_authors",
//   "allAuthors",
//   "res_aut_rinc_letters(ё,й)"
// );

// allOrganizationsIdscience_with_authors("main", "allOrganizationsIdscience");

const asyncFunc = async () => {
  try {
    let aggr = [
      {
        $project: {
          issn: {
            $toUpper: "$issn",
          },
          eissn: {
            $toUpper: "$eissn",
          },
        },
      },
      {
        $group: {
          _id: ["$issn", "$eissn"],
          kol: {
            $sum: 1,
          },
        },
      },
      {
        $project: {
          _id: "$_id",
          issn: "$_id",
          kol: "$kol",
        },
      },
      {
        $unwind: "$issn",
      },
      {
        $match: {
          issn: {
            $ne: "",
          },
        },
      },
      {
        $group: {
          _id: "$_id",
          issn: {
            $push: "$issn",
          },
          kol: {
            $first: "$kol",
          },
        },
      },
      {
        $project: {
          _id: "$d",
          issn: "$issn",
          kol: "$kol",
        },
      },
      {
        $out: "issn_counter_from_scopus",
      },
    ];
    crossref.aggregation("RIEPP", "lab", "slw2022", aggr);
    // crossref.match_doi_scopus_crossref("scopus_works_2017");
    // crossref.Test();
    // crossref.Scopus_list("lab", "crossref", "slw2022", "scopus_jurnal");
    // parsing.GetCrossferJurnalList("crossref", "jurnal");
  } catch (e) {
    console.error(e);
  }
};
measure(asyncFunc)
  .then((report) => {
    // inspect
    const { tSyncOnly, tSyncAsync } = report;
    console.log(`
  ∑ Sync Ops       = ${tSyncOnly / 1000}ms
  ∑ Sync&Async Ops = ${tSyncAsync / 1000}ms
  `);
  })
  .catch((e) => {
    console.error(e);
  });
