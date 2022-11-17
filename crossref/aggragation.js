import * as index from "./index";
/**
 * 1) scopus_works_2017.issn_counter_from_scopus
 *      slw2022.issn_counter_from_scopus
 */
let scopus_works_2017 = {
  issn_counter_from_scopus: [
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
  ],
};

let slw2022 = {
  issn_counter_from_scopus: [
    {
      $match: {
        year: { $gte: "2017" },
      },
    },
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
  ],
};

let issn_counter_from_scopus = {
  issn_counter_copmare: [
    {
      $lookup: {
        from: "issn_counter_from_scopus_RIEPP",
        let: {
          issn: "$issn",
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $eq: [
                  {
                    $sortArray: {
                      input: "$$issn",
                      sortBy: 1,
                    },
                  },
                  {
                    $sortArray: {
                      input: "$issn",
                      sortBy: 1,
                    },
                  },
                ],
              },
            },
          },
        ],
        as: "res",
      },
    },
    {
      $addFields: {
        kol_riepp: "$res.kol",
      },
    },
    {
      $unwind: "$kol_riepp",
    },
    {
      $out: "issn_counter_copmare",
    },
  ],
};

/**
 * снизу создание таблицы
 * 1) создаем issn_sourceid issn с их сорс id
 *  2) удалить {issn:"0000000-"}
 *
 */
let table = {
  locall: {
    sjr: { issn_sourceid: index.sjr_issn_array },
    sjr_works_2017: {
      issn_table_crossref: [
        {
          $project: {
            issn: {
              $map: {
                input: "$ISSN",
                as: "iss",
                in: {
                  $replaceAll: {
                    input: "$$iss",
                    find: "-",
                    replacement: "",
                  },
                },
              },
            },
            push: {
              //   doi: "$DOI",
              //   year: {
              // $substr:  ["$deposited.date-time", 0, 4] поменять на ишьюд,
              // },
              jur: {
                issue: "$issue",
                volume: "$volume",
                pages: "$page",
              },
            },
          },
        },
        {
          $match: {
            "push.year": {
              $gte: "2017",
            },
          },
        },
        {
          $match: {
            "push.year": {
              $lte: "2021",
            },
          },
        },
        {
          $unwind: "$issn",
        },
        {
          $lookup: {
            from: "issn_sourceid",
            let: {
              issn: "$issn",
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $in: ["$$issn", "$issn"],
                  },
                },
              },
              {
                $project: {
                  sourceid: "$sourceid",
                },
              },
            ],
            as: "sourceid",
          },
        },
        {
          $unwind: "$sourceid",
        },
        {
          $addFields: {
            sourceid: "$sourceid.sourceid",
          },
        },
        {
          $unwind: "$sourceid",
        },
        {
          $group: {
            _id: "$sourceid",
            sourceid: {
              $first: "$sourceid",
            },
            issn: {
              $addToSet: "$issn",
            },
            res: {
              $push: "$push",
            },
          },
        },
        {
          $addFields: {
            _id: "$d",
            sourceid: "$_id",
            kol: {
              $size: "$res",
            },
          },
        },
        {
          $out: "issn_table_crossref",
        },
      ],
      issn_table_promejutok: [
        {
          $project: {
            issn: {
              $map: {
                input: "$ISSN",
                as: "iss",
                in: {
                  $replaceAll: {
                    input: "$$iss",
                    find: "-",
                    replacement: "",
                  },
                },
              },
            },
            push: {
              doi: "$DOI",
              year: {
                $substr: ["$deposited.date-time", 0, 4],
              },
              jur: {
                issue: "$issue",
                volume: "$volume",
                pages: "$page",
              },
            },
          },
        },
        {
          $match: {
            "push.year": {
              $gte: "2017",
            },
          },
        },
        {
          $match: {
            "push.year": {
              $lte: "2021",
            },
          },
        },
        {
          $unwind: "$issn",
        },
        {
          $lookup: {
            from: "issn_sourceid",
            let: {
              issn: "$issn",
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $in: ["$$issn", "$issn"],
                  },
                },
              },
              {
                $project: {
                  _id: "$d",
                  sourceid: "$sourceid",
                },
              },
            ],
            as: "sourceid",
          },
        },
        {
          $unwind: "$sourceid",
        },
        {
          $addFields: {
            sourceid: "$sourceid.sourceid",
          },
        },
        {
          $unwind: "$sourceid",
        },
        {
          $addFields: {
            _id: "$d",
          },
        },
        {
          $out: "issn_table_promejutok",
        },
      ],
    },
    issn_sourceid: {
      issn_table_match: [
        {
          $unwind: "$sourceid",
        },
        {
          $lookup: {
            from: "scopus_works_2017",
            let: {
              sourceid: "$sourceid",
            },
            pipeline: [
              {
                $match: {
                  year: {
                    $gte: "2017",
                  },
                },
              },
              {
                $match: {
                  year: {
                    $lte: "2021",
                  },
                },
              },
              {
                $match: {
                  $expr: {
                    $eq: ["$sourceid", "$$sourceid"],
                  },
                },
              },
              {
                $project: {
                  _id: "$_id",
                  issn: "$issn",
                  eissn: "$eissn",
                  doi: "$doi",
                  year: "$year",
                  jur: {
                    issue: "$issue",
                    volume: "$volume",
                    pages: "$pages",
                  },
                },
              },
            ],
            as: "res",
          },
        },
        {
          $addFields: {
            kol: {
              $size: "$res",
            },
          },
        },
        {
          $out: "issn_table_match",
        },
      ],
      table: [
        {
          $addFields: {
            years: {
              2017: {
                crossref: 0,
              },
              2018: {
                crossref: 0,
              },
              2019: {
                crossref: 0,
              },
              2020: {
                crossref: 0,
              },
              2021: {
                crossref: 0,
              },
              2022: {
                crossref: 0,
              },
            },
          },
        },
        {
          $lookup: {
            from: "table_crossref",
            localField: "sourceid",
            foreignField: "sourceid",
            as: "crossref",
          },
        },
        {
          $addFields: {
            crossref: {
              $reduce: {
                input: "$crossref",
                initialValue: 0,
                in: {
                  $sum: [
                    "$$value",
                    {
                      $size: "$$this.crossref",
                    },
                  ],
                },
              },
            },
            years: {
              2017: {
                crossref: {
                  $reduce: {
                    input: "$crossref",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.crossref",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", 2017],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2018: {
                crossref: {
                  $reduce: {
                    input: "$crossref",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.crossref",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", 2018],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2019: {
                crossref: {
                  $reduce: {
                    input: "$crossref",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.crossref",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", 2019],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2020: {
                crossref: {
                  $reduce: {
                    input: "$crossref",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.crossref",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", 2020],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2021: {
                crossref: {
                  $reduce: {
                    input: "$crossref",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.crossref",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", 2021],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2022: {
                crossref: {
                  $reduce: {
                    input: "$crossref",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.crossref",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", 2022],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
            },
          },
        },
        {
          $lookup: {
            from: "table_match",
            localField: "sourceid",
            foreignField: "sourceid",
            as: "match",
          },
        },
        {
          $addFields: {
            match: {
              $reduce: {
                input: "$match",
                initialValue: 0,
                in: {
                  $sum: [
                    "$$value",
                    {
                      $size: "$$this.match",
                    },
                  ],
                },
              },
            },
            "match>=2017": {
              $reduce: {
                input: "$match",
                initialValue: 0,
                in: {
                  $sum: [
                    "$$value",
                    {
                      $size: "$$this.match>=2017",
                    },
                  ],
                },
              },
            },
            "match<2017": {
              $reduce: {
                input: "$match",
                initialValue: 0,
                in: {
                  $sum: [
                    "$$value",
                    {
                      $size: "$$this.match<2017",
                    },
                  ],
                },
              },
            },
            years: {
              2017: {
                match: {
                  $reduce: {
                    input: "$match",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.match>=2017",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2017"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2018: {
                match: {
                  $reduce: {
                    input: "$match",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.match>=2017",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2018"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2019: {
                match: {
                  $reduce: {
                    input: "$match",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.match>=2017",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2019"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2020: {
                match: {
                  $reduce: {
                    input: "$match",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.match>=2017",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2020"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2021: {
                match: {
                  $reduce: {
                    input: "$match",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.match>=2017",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2021"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2022: {
                match: {
                  $reduce: {
                    input: "$match",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.match>=2017",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2022"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
            },
          },
        },
        {
          $lookup: {
            from: "table_scopus",
            localField: "sourceid",
            foreignField: "sourceid",
            as: "scopus_doi1",
          },
        },
        {
          $addFields: {
            scopus_doi1: {
              $reduce: {
                input: "$scopus_doi1",
                initialValue: 0,
                in: {
                  $sum: [
                    "$$value",
                    {
                      $size: "$$this.res_doi1",
                    },
                  ],
                },
              },
            },
            scopus_doi0: {
              $reduce: {
                input: "$scopus_doi1",
                initialValue: 0,
                in: {
                  $sum: [
                    "$$value",
                    {
                      $size: "$$this.res_doi0",
                    },
                  ],
                },
              },
            },
            years: {
              2017: {
                scopus_doi1: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi1",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2017"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                scopus_doi0: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi0",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2017"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2018: {
                scopus_doi1: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi1",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2018"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                scopus_doi0: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi0",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2018"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2019: {
                scopus_doi1: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi1",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2019"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                scopus_doi0: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi0",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2019"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2020: {
                scopus_doi1: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi1",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2020"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                scopus_doi0: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi0",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2020"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2021: {
                scopus_doi1: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi1",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2021"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                scopus_doi0: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi0",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2021"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
              2022: {
                scopus_doi1: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi1",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2022"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
                scopus_doi0: {
                  $reduce: {
                    input: "$scopus_doi1",
                    initialValue: 0,
                    in: {
                      $sum: [
                        "$$value",
                        {
                          $size: {
                            $filter: {
                              input: "$$this.res_doi0",
                              as: "item",
                              cond: {
                                $eq: ["$$item.year", "2022"],
                              },
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
            },
          },
        },
        {
          $out: "table",
        },
      ],
    },
    issn_table_scopus: {
      table_all: [
        {
          $unwind: "$res",
        },
        {
          $group: {
            _id: {
              year: "$res.year",
              sourceid: "$sourceid",
            },
            kol_scopus: {
              $sum: 1,
            },
          },
        },
        {
          $addFields: {
            year: "$_id.year",
          },
        },
        {
          $out: "table_all",
        },
      ],
    },
    scopus_works_2017: {
      //merge to table_all
      table_all: [
        {
          $match: {
            year: {
              $gte: "2017",
            },
          },
        },
        {
          $match: {
            year: {
              $lte: "2021",
            },
          },
        },
        {
          $project: {
            _id: "$_id",
            issn: "$issn",
            eissn: "$eissn",
            doi: "$doi",
            year: "$year",
            sourceid: "$sourceid",
            jur: {
              issue: "$issue",
              volume: "$volume",
              pages: "$pages",
            },
          },
        },
        {
          $group: {
            _id: {
              year: "$year",
              sourceid: "$sourceid",
            },
            kol_match: {
              $sum: 1,
            },
          },
        },
        {
          $addFields: {
            year: "$_id.year",
          },
        },
        {
          $merge: "table_all",
        },
      ],
    },
    scopus_works_2017_all: {
      issn_table_scopus_works_all: [
        {
          $project: {
            sourceid: "$sourceid",
            issn: {
              $reduce: {
                input: {
                  $sortArray: {
                    input: ["$issn", "$eissn"],
                    sortBy: 1,
                  },
                },
                initialValue: [],
                in: {
                  $concatArrays: [
                    "$$value",
                    {
                      $cond: ["$$this", ["$$this"], []],
                    },
                  ],
                },
              },
            },
            res: {
              _id: "$_id",
              issn: "$issn",
              eissn: "$eissn",
              doi: "$doi",
              year: "$year",
              jur: {
                issue: "$issue",
                volume: "$volume",
                pages: "$pages",
              },
            },
          },
        },
        {
          $out: "issn_table_scopus_works_all",
        },
      ],
    },
    issn_table_promejutok: {
      issn_table_promejutok2: [],
    },
    issn_table_promejutok2: {
      table_all: [
        {
          $unwind: "$res",
        },
        {
          $group: {
            _id: {
              year: "$res.year",
              sourceid: "$sourceid",
            },
            kol_crossref: {
              $sum: 1,
            },
          },
        },
        {
          $addFields: {
            year: "$_id.year",
          },
        },
        {
          $merge: "table_all",
        },
      ],
    },
    table_all: {
      table_new2: [
        {
          $addFields: {
            scopus: {
              $cond: [
                {
                  $eq: ["$kol_scopus", "$d"],
                },
                0,
                "$kol_scopus",
              ],
            },
            match: {
              $cond: [
                {
                  $eq: ["$kol_match", "$d"],
                },
                0,
                "$kol_match",
              ],
            },
            crossref: {
              $cond: [
                {
                  $eq: ["$kol_crossref", "$d"],
                },
                0,
                "$kol_crossref",
              ],
            },
            kol_crossref: "$d",
            kol_match: "$d",
            kol_scopus: "$d",
          },
        },
        {
          $addFields: {
            crossref: {
              $sum: ["$kol_2022_cross", "$crossref"],
            },
            kol_2022_cross: "$d",
            match: {
              $sum: ["$match", "$kol_match_dop"],
            },
            kol_match_dop: "$d",
          },
        },
        {
          $addFields: {
            scopus: {
              $sum: [
                "$scopus",
                {
                  $multiply: [-1, "$match"],
                },
              ],
            },
            crossref: {
              $sum: [
                "$crossref",
                {
                  $multiply: [-1, "$match"],
                },
              ],
            },
          },
        },
        {
          $out: "table_new2",
        },
      ],
    },
    issn_table_scopus_2017_2022: {
      table_scopus: [
        {
          $group: {
            _id: "$sourceid",
            sourceid: {
              $first: "$sourceid",
            },
            issn: {
              $first: "$issn",
            },
            res_doi1: {
              $push: {
                $cond: ["$exist_doi", "$res", "$d"],
              },
            },
            res_doi0: {
              $push: {
                $cond: ["$exist_doi", "$d", "$res"],
              },
            },
          },
        },
        {
          $addFields: {
            _id: "$d",
          },
        },
        {
          $out: "table_scopus",
        },
      ],
    },
    issn_table_scopus_works_all: {
      table_match: [
        {
          $lookup: {
            from: "issn_sourceid",
            localField: "issn",
            foreignField: "issn",
            as: "sourceid_base",
          },
        },
        {
          $addFields: {
            sourceid: {
              $cond: [
                "$sourceid",
                "$sourceid",
                {
                  $arrayElemAt: [
                    {
                      $arrayElemAt: ["$sourceid_base.sourceid", 0],
                    },
                    0,
                  ],
                },
              ],
            },
          },
        },
        {
          $group: {
            _id: "$sourceid",
            sourceid: {
              $first: "$sourceid",
            },
            issn: {
              $addToSet: "$issn",
            },
            match: {
              $push: "$res",
            },
            "match>=2017": {
              $push: {
                $cond: [
                  {
                    $gte: ["$res.year", "2017"],
                  },
                  "$res",
                  "$d",
                ],
              },
            },
            "match<2017": {
              $push: {
                $cond: [
                  {
                    $lt: ["$res.year", "2017"],
                  },
                  "$res",
                  "$d",
                ],
              },
            },
          },
        },
        {
          $addFields: {
            issn: {
              $reduce: {
                input: "$issn",
                initialValue: [],
                in: {
                  $setUnion: ["$$value", "$$this"],
                },
              },
            },
          },
        },
        {
          $addFields: {
            _id: "$d",
          },
        },
        {
          $out: "table_match",
        },
      ],
    },
    sjr_works_2017_scourceid: {
      issn_table_sjr_works_2017_scourceid: [
        {
          $project: {
            sourceid: {
              $sortArray: {
                input: "$sourceid",
                sortBy: 1,
              },
            },
            issn: "$sourceid_issn",
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
                      $arrayElemAt: [
                        "$journal-issue.published-online.date-parts",
                        0,
                      ],
                    },
                    {
                      $arrayElemAt: [
                        "$journal-issue.published-online.date-parts",
                        0,
                      ],
                    },
                    {
                      $arrayElemAt: ["$published-online.date-parts", 0],
                    },
                  ],
                },
                "published-print": {
                  $cond: [
                    {
                      $arrayElemAt: [
                        "$journal-issue.published-print.date-parts",
                        0,
                      ],
                    },
                    {
                      $arrayElemAt: [
                        "$journal-issue.published-print.date-parts",
                        0,
                      ],
                    },
                    {
                      $arrayElemAt: ["$published-print.date-parts", 0],
                    },
                  ],
                },
              },
              jur: {
                issue: "$issue",
                volume: "$volume",
                pages: "$page",
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
          $out: "issn_table_sjr_works_2017_scourceid",
        },
      ],
    },
    issn_table_sjr_works_2017_scourceid: {
      table_crossref: [
        {
          $group: {
            _id: "$sourceid",
            sourceid: {
              $first: "$sourceid",
            },
            issn: {
              $addToSet: "$issn",
            },
            crossref: {
              $push: "$res",
            },
          },
        },
        {
          $addFields: {
            issn: {
              $reduce: {
                input: "$issn",
                initialValue: [],
                in: {
                  $setUnion: ["$$value", "$$this"],
                },
              },
            },
          },
        },
        {
          $addFields: {
            _id: "$d",
          },
        },
        {
          $out: "table_crossref",
        },
      ],
    },
  },
  reipp: {
    kostuk_issn_sourceid: {
      kostuk_issn_table_scopus: [
        {
          $unwind: "$sourceid",
        },
        {
          $lookup: {
            from: "slw2022",
            let: {
              sourceid: "$sourceid",
            },
            pipeline: [
              {
                $match: {
                  year: {
                    $gte: "2016",
                  },
                },
              },
              {
                $match: {
                  year: {
                    $lte: "2022",
                  },
                },
              },
              {
                $match: {
                  $expr: {
                    $eq: ["$sourceid", "$$sourceid"],
                  },
                },
              },
              {
                $project: {
                  _id: "$_id",
                  issn: "$issn",
                  eissn: "$eissn",
                  doi: "$doi",
                  year: "$year",
                  jur: {
                    issue: "$issue",
                    volume: "$volume",
                    pages: "$pages",
                  },
                },
              },
            ],
            as: "res",
          },
        },
        {
          $addFields: {
            kol: {
              $size: "$res",
            },
          },
        },
        {
          $out: "kostuk_issn_table_scopus_2016-2022",
        },
      ],
      kostuk_issn_table_scopus_2016_2022: [
        {
          $unwind: "$sourceid",
        },
        {
          $lookup: {
            from: "slw2022",
            let: {
              sourceid: "$sourceid",
            },
            pipeline: [
              {
                $match: {
                  year: {
                    $gte: "2016",
                  },
                },
              },
              {
                $match: {
                  year: {
                    $lte: "2022",
                  },
                },
              },
              {
                $match: {
                  $expr: {
                    $eq: ["$sourceid", "$$sourceid"],
                  },
                },
              },
              {
                $project: {
                  _id: "$_id",
                  issn: "$issn",
                  eissn: "$eissn",
                  doi: "$doi",
                  year: "$year",
                  jur: {
                    issue: "$issue",
                    volume: "$volume",
                    pages: "$pages",
                  },
                },
              },
            ],
            as: "res",
          },
        },
        {
          $unwind: {
            path: "$res",
            preserveNullAndEmptyArrays: true,
          },
        },
        {
          $addFields: {
            _id: "$d",
            exist_doi: {
              $cond: ["$res.doi", true, false],
            },
          },
        },
        {
          $out: "kostuk_issn_table_scopus_2016_2022",
        },
      ],
    },
    kostuk_issn_table_scopus_2016_2022: {
      kostuk_issn_table_scopus_2016_2022_download: [
        {
          $group: {
            _id: {
              issn: "$issn",
              exist_doi: "$exist_doi",
            },
            sourceid: {
              $addToSet: "$sourceid",
            },
            issn: {
              $first: "$issn",
            },
            res: {
              $addToSet: "$res",
            },
          },
        },
        {
          $addFields: {
            _id: "$d",
            exist_doi: "$_id.exist_doi",
          },
        },
        {
          $out: "kostuk_issn_table_scopus_2016_2022_download",
        },
      ],
    },
  },
};

let transfer_crossref_to_scopus = [
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

export { table };
