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

//сверка двух массивов с разным порядком
let t = {
  from: "test",
  let: { issn: "$issn" },
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
};
