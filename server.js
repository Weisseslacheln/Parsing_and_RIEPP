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
/**
 * ищем ошибки на 700к
 * разбить скопус на те где есть дои и нет дои (те добавить два поля)
 * убираем разбиение по году, те только по сорсайди
 * проверить ишьед и принт онлай может заменить
 *
 *
 */
let aggr = [
  {
    $project: {
      abstract: "$abstract",
      affiliation: {
        $reduce: {
          input: "$author",
          initialValue: [],
          in: {
            $setUnion: ["$$value", "$$this.affiliation"],
          },
        },
      },
      author: {
        $map: {
          input: {
            $concatArrays: [
              {
                $filter: {
                  input: "$author",
                  as: "el",
                  cond: {
                    $eq: ["$$el.sequence", "first"],
                  },
                },
              },
              {
                $filter: {
                  input: "$author",
                  as: "el",
                  cond: {
                    $not: {
                      $eq: ["$$el.sequence", "first"],
                    },
                  },
                },
              },
            ],
          },
          as: "el",
          in: {
            name: {
              $cond: [
                {
                  $and: [
                    {
                      $eq: ["$$el.family", "$delete"],
                    },
                    {
                      $eq: ["$$el.given", "$delete"],
                    },
                  ],
                },
                "$$el.name",
                {
                  $concat: [
                    "$$el.family",
                    " ",
                    {
                      $reduce: {
                        input: {
                          $split: ["$$el.given", " "],
                        },
                        initialValue: "",
                        in: {
                          $concat: [
                            "$$value",
                            {
                              $substrCP: ["$$this", 0, 1],
                            },
                            ".",
                          ],
                        },
                      },
                    },
                  ],
                },
              ],
            },
            surname: "$$el.family",
            given: "$$el.given",
            affiliation: "$$el.affiliation",
            ORCID: {
              $cond: [
                {
                  $eq: ["$$el.ORCID", "$delte"],
                },
                "$delete",
                {
                  $arrayElemAt: [
                    {
                      $split: ["$$el.ORCID", "http://orcid.org/"],
                    },
                    1,
                  ],
                },
              ],
            },
          },
        },
      },
      authorcount: {
        $cond: [
          "$author",
          {
            $size: "$author",
          },
          0,
        ],
      },
      doctype: {
        $concat: [
          {
            $toUpper: {
              $substrCP: [
                {
                  $ifNull: [
                    {
                      $arrayElemAt: [
                        {
                          $split: ["$type", "-"],
                        },
                        1,
                      ],
                    },
                    "$type",
                  ],
                },
                0,
                1,
              ],
            },
          },
          {
            $substrCP: [
              {
                $ifNull: [
                  {
                    $arrayElemAt: [
                      {
                        $split: ["$type", "-"],
                      },
                      1,
                    ],
                  },
                  "$type",
                ],
              },
              1,
              {
                $subtract: [
                  {
                    $strLenCP: {
                      $ifNull: [
                        {
                          $arrayElemAt: [
                            {
                              $split: ["$type", "-"],
                            },
                            1,
                          ],
                        },
                        "$type",
                      ],
                    },
                  },
                  1,
                ],
              },
            ],
          },
        ],
      },
      doi: "$DOI",
      eissn: {
        $cond: [
          {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: "$issn-type",
                    as: "el",
                    cond: {
                      $eq: ["$$el.type", "electronic"],
                    },
                  },
                },
              },
              0,
            ],
          },
          {
            $arrayElemAt: [
              {
                $map: {
                  input: {
                    $filter: {
                      input: "$issn-type",
                      as: "el",
                      cond: {
                        $eq: ["$$el.type", "electronic"],
                      },
                    },
                  },
                  as: "el",
                  in: {
                    $replaceAll: {
                      input: "$$el.value",
                      find: "-",
                      replacement: "",
                    },
                  },
                },
              },
              0,
            ],
          },
          "$no",
        ],
      },
      issn: {
        $cond: [
          {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: "$issn-type",
                    as: "el",
                    cond: {
                      $eq: ["$$el.type", "electronic"],
                    },
                  },
                },
              },
              0,
            ],
          },
          {
            $arrayElemAt: [
              {
                $map: {
                  input: {
                    $filter: {
                      input: "$issn-type",
                      as: "el",
                      cond: {
                        $eq: ["$$el.type", "print"],
                      },
                    },
                  },
                  as: "el",
                  in: {
                    $replaceAll: {
                      input: "$$el.value",
                      find: "-",
                      replacement: "",
                    },
                  },
                },
              },
              0,
            ],
          },
          "$no",
        ],
      },
      issue: {
        $cond: [
          "$issue",
          "$issue",
          {
            $cond: ["$journal-issue.issue", "$journal-issue.issue", "$delete"],
          },
        ],
      },
      index: {
        $toDate: "$indexed.timestamp",
      },
      pages: "$page",
      source: {
        full: "$container-title",
        short: "$short-container-title",
      },
      srctype: {
        $concat: [
          {
            $toUpper: {
              $substrCP: [
                {
                  $arrayElemAt: [
                    {
                      $split: ["$type", "-"],
                    },
                    0,
                  ],
                },
                0,
                1,
              ],
            },
          },
          {
            $substrCP: [
              {
                $arrayElemAt: [
                  {
                    $split: ["$type", "-"],
                  },
                  0,
                ],
              },
              1,
              {
                $subtract: [
                  {
                    $strLenCP: {
                      $arrayElemAt: [
                        {
                          $split: ["$type", "-"],
                        },
                        0,
                      ],
                    },
                  },
                  1,
                ],
              },
            ],
          },
        ],
      },
      title: {
        $cond: [
          {
            $eq: ["$subtitle", "$delete"],
          },
          {
            $arrayElemAt: ["$title", 0],
          },
          {
            $concat: [
              {
                $arrayElemAt: ["$title", 0],
              },
              ": ",
              {
                $arrayElemAt: ["$subtitle", 0],
              },
            ],
          },
        ],
      },
      year: {
        created: {
          $arrayElemAt: ["$created.date-parts", 0],
        },
        issued: {
          $arrayElemAt: ["$issued.date-parts", 0],
        },
        published: {
          $arrayElemAt: ["$published.date-parts", 0],
        },
        "published-online": {
          $cond: [
            {
              $arrayElemAt: ["$journal-issue.published-online.date-parts", 0],
            },
            {
              $arrayElemAt: ["$journal-issue.published-online.date-parts", 0],
            },
            {
              $arrayElemAt: ["$published-online.date-parts", 0],
            },
          ],
        },
        "published-print": {
          $cond: [
            {
              $arrayElemAt: ["$journal-issue.published-print.date-parts", 0],
            },
            {
              $arrayElemAt: ["$journal-issue.published-print.date-parts", 0],
            },
            {
              $arrayElemAt: ["$published-print.date-parts", 0],
            },
          ],
        },
      },
      volume: "$volume",
      crossref: {
        date: {
          created: {
            $cond: [
              "$created",
              {
                $toDate: "$created.timestamp",
              },
              "$delete",
            ],
          },
          deposited: {
            $cond: [
              "$deposited",
              {
                $toDate: "$deposited.timestamp",
              },
              "$delete",
            ],
          },
          issued: {
            $cond: [
              "$issued",
              {
                $reduce: {
                  input: {
                    $slice: [
                      {
                        $arrayElemAt: ["$issued.date-parts", 0],
                      },
                      1,
                      2,
                    ],
                  },
                  initialValue: {
                    $toString: {
                      $arrayElemAt: [
                        {
                          $arrayElemAt: ["$issued.date-parts", 0],
                        },
                        0,
                      ],
                    },
                  },
                  in: {
                    $concat: [
                      "$$value",
                      "-",
                      {
                        $toString: "$$this",
                      },
                    ],
                  },
                },
              },
              "$delete",
            ],
          },
          published: {
            $cond: [
              "$published",
              {
                $reduce: {
                  input: {
                    $slice: [
                      {
                        $arrayElemAt: ["$published.date-parts", 0],
                      },
                      1,
                      2,
                    ],
                  },
                  initialValue: {
                    $toString: {
                      $arrayElemAt: [
                        {
                          $arrayElemAt: ["$published.date-parts", 0],
                        },
                        0,
                      ],
                    },
                  },
                  in: {
                    $concat: [
                      "$$value",
                      "-",
                      {
                        $toString: "$$this",
                      },
                    ],
                  },
                },
              },
              "$delete",
            ],
          },
          "published-online": {
            $cond: [
              "$published-online",
              {
                $reduce: {
                  input: {
                    $slice: [
                      {
                        $arrayElemAt: ["$published-online.date-parts", 0],
                      },
                      1,
                      2,
                    ],
                  },
                  initialValue: {
                    $toString: {
                      $arrayElemAt: [
                        {
                          $arrayElemAt: ["$published-online.date-parts", 0],
                        },
                        0,
                      ],
                    },
                  },
                  in: {
                    $concat: [
                      "$$value",
                      "-",
                      {
                        $toString: "$$this",
                      },
                    ],
                  },
                },
              },
              {
                $cond: [
                  "$journal-issue.published-online",
                  {
                    $reduce: {
                      input: {
                        $slice: [
                          {
                            $arrayElemAt: [
                              "$journal-issue.published-online.date-parts",
                              0,
                            ],
                          },
                          1,
                          2,
                        ],
                      },
                      initialValue: {
                        $toString: {
                          $arrayElemAt: [
                            {
                              $arrayElemAt: [
                                "$journal-issue.published-online.date-parts",
                                0,
                              ],
                            },
                            0,
                          ],
                        },
                      },
                      in: {
                        $concat: [
                          "$$value",
                          "-",
                          {
                            $toString: "$$this",
                          },
                        ],
                      },
                    },
                  },
                  "$delete",
                ],
              },
            ],
          },
          "published-other": {
            $cond: [
              "$published-other",
              {
                $reduce: {
                  input: {
                    $slice: [
                      {
                        $arrayElemAt: ["$published-other.date-parts", 0],
                      },
                      1,
                      2,
                    ],
                  },
                  initialValue: {
                    $toString: {
                      $arrayElemAt: [
                        {
                          $arrayElemAt: ["$published-other.date-parts", 0],
                        },
                        0,
                      ],
                    },
                  },
                  in: {
                    $concat: [
                      "$$value",
                      "-",
                      {
                        $toString: "$$this",
                      },
                    ],
                  },
                },
              },
              "$delete",
            ],
          },
          "published-print": {
            $cond: [
              "$published-print",
              {
                $reduce: {
                  input: {
                    $slice: [
                      {
                        $arrayElemAt: ["$published-print.date-parts", 0],
                      },
                      1,
                      2,
                    ],
                  },
                  initialValue: {
                    $toString: {
                      $arrayElemAt: [
                        {
                          $arrayElemAt: ["$published-print.date-parts", 0],
                        },
                        0,
                      ],
                    },
                  },
                  in: {
                    $concat: [
                      "$$value",
                      "-",
                      {
                        $toString: "$$this",
                      },
                    ],
                  },
                },
              },
              {
                $cond: [
                  "$journal-issue.published-print",
                  {
                    $reduce: {
                      input: {
                        $slice: [
                          {
                            $arrayElemAt: [
                              "$journal-issue.published-print.date-parts",
                              0,
                            ],
                          },
                          1,
                          2,
                        ],
                      },
                      initialValue: {
                        $toString: {
                          $arrayElemAt: [
                            {
                              $arrayElemAt: [
                                "$journal-issue.published-print.date-parts",
                                0,
                              ],
                            },
                            0,
                          ],
                        },
                      },
                      in: {
                        $concat: [
                          "$$value",
                          "-",
                          {
                            $toString: "$$this",
                          },
                        ],
                      },
                    },
                  },
                  "$delete",
                ],
              },
            ],
          },
        },
        collective: "$author.name",
        editor: "$editor",
        funder: "$funder",
        ISBN: "$ISBN",
        "isbn-type": "$isbn-type",
        language: "$language",
        "original-title": "$original-title",
        prefix: "$prefix",
        publisher: "$publisher",
        reference: "$reference",
        subject: "$subject",
        "update-to": "$update-to",
      },
    },
  },
  {
    $addFields: {
      year: {
        $cond: [
          "$year.published-print",
          {
            $arrayElemAt: ["$year.published-print", 0],
          },
          {
            $cond: [
              "$year.published-online",
              {
                $arrayElemAt: ["$year.published-online", 0],
              },
              {
                $cond: [
                  "$year.published",
                  {
                    $arrayElemAt: ["$year.published", 0],
                  },
                  {
                    $cond: [
                      "$year.issued",
                      {
                        $arrayElemAt: ["$year.issued", 0],
                      },
                      {
                        $cond: [
                          "$year.created",
                          {
                            $arrayElemAt: ["$year.created", 0],
                          },
                          "",
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
      "crossref.date.issued": {
        $cond: [
          "$crossref.date.issued",
          {
            $cond: [
              {
                $eq: [
                  {
                    $strLenCP: "$crossref.date.issued",
                  },
                  4,
                ],
              },
              {
                $toDate: {
                  $concat: ["$crossref.date.issued", "-01"],
                },
              },
              {
                $toDate: "$crossref.date.issued",
              },
            ],
          },
          "$delete",
        ],
      },
      "crossref.date.published": {
        $cond: [
          "$crossref.date.published",
          {
            $cond: [
              {
                $eq: [
                  {
                    $strLenCP: "$crossref.date.published",
                  },
                  4,
                ],
              },
              {
                $toDate: {
                  $concat: ["$crossref.date.published", "-01"],
                },
              },
              {
                $toDate: "$crossref.date.published",
              },
            ],
          },
          "$delete",
        ],
      },
      "crossref.date.published-online": {
        $cond: [
          "$crossref.date.published-online",
          {
            $cond: [
              {
                $eq: [
                  {
                    $strLenCP: "$crossref.date.published-online",
                  },
                  4,
                ],
              },
              {
                $toDate: {
                  $concat: ["$crossref.date.published-online", "-01"],
                },
              },
              {
                $toDate: "$crossref.date.published-online",
              },
            ],
          },
          "$delete",
        ],
      },
      "crossref.date.published-other": {
        $cond: [
          "$crossref.date.published-other",
          {
            $cond: [
              {
                $eq: [
                  {
                    $strLenCP: "$crossref.date.published-other",
                  },
                  4,
                ],
              },
              {
                $toDate: {
                  $concat: ["$crossref.date.published-other", "-01"],
                },
              },
              {
                $toDate: "$crossref.date.published-other",
              },
            ],
          },
          "$delete",
        ],
      },
      "crossref.date.published-print": {
        $cond: [
          "$crossref.date.published-print",
          {
            $cond: [
              {
                $eq: [
                  {
                    $strLenCP: "$crossref.date.published-print",
                  },
                  4,
                ],
              },
              {
                $toDate: {
                  $concat: ["$crossref.date.published-print", "-01"],
                },
              },
              {
                $toDate: "$crossref.date.published-print",
              },
            ],
          },
          "$delete",
        ],
      },
    },
  },
  {
    $out: "transfer_crossref_to_scopus",
  },
];

let aggregation = [
  {
    $project: {
      res: {
        doi: "$DOI",
        years: {
          created: {
            $arrayElemAt: ["$created.date-parts", 0],
          },
          issued: {
            $arrayElemAt: ["$issued.date-parts", 0],
          },
          published: {
            $arrayElemAt: ["$published.date-parts", 0],
          },
          "published-online": {
            $cond: [
              {
                $arrayElemAt: ["$journal-issue.published-online.date-parts", 0],
              },
              {
                $arrayElemAt: ["$journal-issue.published-online.date-parts", 0],
              },
              {
                $arrayElemAt: ["$published-online.date-parts", 0],
              },
            ],
          },
          "published-print": {
            $cond: [
              {
                $arrayElemAt: ["$journal-issue.published-print.date-parts", 0],
              },
              {
                $arrayElemAt: ["$journal-issue.published-print.date-parts", 0],
              },
              {
                $arrayElemAt: ["$published-print.date-parts", 0],
              },
            ],
          },
        },
      },
    },
  },
  {
    $addFields: {
      _id: "$d",
      "res.year": {
        $cond: [
          "$res.years.published-print",
          {
            $arrayElemAt: ["$res.years.published-print", 0],
          },
          {
            $cond: [
              "$res.years.published-online",
              {
                $arrayElemAt: ["$res.years.published-online", 0],
              },
              {
                $cond: [
                  "$res.years.published",
                  {
                    $arrayElemAt: ["$res.years.published", 0],
                  },
                  {
                    $cond: [
                      "$res.years.issued",
                      {
                        $arrayElemAt: ["$res.years.issued", 0],
                      },
                      {
                        $cond: [
                          "$res.years.created",
                          {
                            $arrayElemAt: ["$res.years.created", 0],
                          },
                          "",
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    },
  },
  {
    $match: {
      "res.year": 2021,
    },
  },
  {
    $project: {
      doi_low: {
        $toLower: "$res.doi",
      },
      doi: "$res.doi",
    },
  },
  {
    $lookup: {
      from: "scopus_works_2017_all",
      localField: "doi",
      foreignField: "doi_low",
      as: "result",
    },
  },
  {
    $match: {
      result: [],
    },
  },
  {
    $sample: {
      size: 100,
    },
  },
  {
    $lookup: {
      from: "sjr_works_2017",
      localField: "doi",
      foreignField: "_id",
      as: "result",
    },
  },
  {
    $unwind: "$result",
  },
  {
    $addFields: {
      array: {
        $concatArrays: [
          {
            $objectToArray: "$result",
          },
        ],
      },
    },
  },
  {
    $replaceRoot: {
      newRoot: {
        $arrayToObject: "$array",
      },
    },
  },
  {
    $project: {
      DOI: "$DOI",
      title: "$title",
      subtitle: "$subtitle",
    },
  },
  {
    $out: "100_test",
  },
];

let now = [
  {
    $project: {
      scopus: {
        sourceid: "$sourceid",
      },
    },
  },
  {
    $merge: "transfer_crossref_to_scopus",
  },
];

crossref.aggregation("Locall", "crossref", "sjr_works_2017_scourceid", now);

// crossref.match_doi_scopus_crossref("scopus_works_2017_doi_low");
// crossref.aggregation(
//   "RIEPP",
//   "lab",
//   "kostuk_issn_table_scopus_2016_2022",
//   aggr
// );

// const asyncFunc = async () => {
//   try {
//     // crossref.aggregation(
//     //   "RIEPP",
//     //   "lab",
//     //   "kostuk_issn",
//     //   table.kostuk_issn.kostuk_issn_table_scopus
//     // );
//     //
//     // crossref.test_connection();
//     // parsing.GetCrossferJurnalList("crossref", "jurnal");
//   } catch (e) {
//     console.error(e);
//   }
// };
// measure(asyncFunc)
//   .then((report) => {
//     // inspect
//     const { tSyncOnly, tSyncAsync } = report;
//     console.log(`
//   ∑ Sync Ops       = ${tSyncOnly / 1000}ms
//   ∑ Sync&Async Ops = ${tSyncAsync / 1000}ms
//   `);
//   })
//   .catch((e) => {
//     console.error(e);
//   });

// import * as sqlitemongo from "sqlitemongo ";

// console.log(sqlitemongo);
// async function test() {
//   var sqlitePath = "./test.sqlite3";
//   var mongoURI = "mongodb://127.0.0.1:27017/sql";
//   var mongoDbName = "test-database";
//   await sqlitemongo(sqlitePath, mongoURI, mongoDbName /* optional */);
// }
// test().catch(console.error);
