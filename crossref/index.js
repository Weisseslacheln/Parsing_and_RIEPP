import {
  GetBase,
  Update,
  GetBase_RIEPP,
  Update_RIEPP,
  Save,
} from "../db/utils";
import fs from "fs";
import fetch from "node-fetch-retry";
import cliProgress from "cli-progress";

export async function Scopus_list(base_RIEPP, base, inColl, outColl) {
  let Scopus_list = await GetBase_RIEPP(base_RIEPP, inColl, [
    {
      $match: {
        eissn: "23129972",
      },
    },
  ]);
  await Update(Scopus_list, base, outColl);
}

export async function aggregation(server, base, coll, aggregation) {
  server == "RIEPP"
    ? await GetBase_RIEPP(base, coll, aggregation)
    : await GetBase(base, coll, aggregation);
}

export async function Test() {
  let test = fs.readFileSync("C:/agri/parsing/crossref/9998.json");
  await Save(JSON.parse(test).items, "crossref", "april2022");
}

function write_zeros(len) {
  // добавляет нули в начало issn
  let zeros = "";
  for (let i = 0; i < 8 - len; i++) {
    zeros += "0";
  }
  return zeros;
}

export async function sjr_issn_array() {
  //функция которая создает списки issn журналов по публикациям
  /**
   * для этой базы работает быстро лучше не создавать коллекцию
   * aggregation sjr(lab) for coll sjr(crossref)
   * {"category.group" : { $in : ["Social Sciences", "Economics, Econometrics and Finance", "Business, Management and Accounting"] } }
   */
  let issn = await GetBase("crossref", "sjr");
  let data = [[]];
  //испрвление 7ми значного isnn, оказывается есть не только 7ми есть и 6ти

  issn = issn.map((el) => {
    return {
      issn: el.issn.map((elem) =>
        elem.length != 8 ? `${write_zeros(elem.length)}${elem}` : elem
      ),
    };
  });
  //
  let match = {};
  for (let i in issn) {
    match = data.findIndex((el) =>
      issn[i].issn
        .map((elem) => {
          return el.findIndex((element) => element == elem) != -1;
        })
        .find((elem) => elem)
    );
    if (match != -1) {
      data[match] = [...new Set(data[match].concat(issn[i].issn))];
    } else {
      if (data[0].length == 0) {
        data = [issn[i].issn];
      } else {
        data.push(issn[i].issn);
      }
    }
  }
  await Save(
    data.map((el) => {
      return {
        issn: el,
      };
    }),
    "crossref",
    "issn_2"
  );
}

async function crossref_cursor(coll, url, cursor) {
  await fetch(cursor == "" ? url : url.slice(0, -1) + cursor, {
    method: "GET",
    retry: 5,
    pause: 2000,
    callback: (retry) => {
      console.log(`Trying: ${retry}`);
    },
  })
    .then((response) => response.json())
    .then(async (data) => {
      if (data.status == "ok") {
        if (data.message.items.length != 0) {
          await Update(
            data.message.items.map((el) => {
              return {
                ...el,
                _id: el.DOI,
              };
            }),
            "crossref",
            coll
          );
          await crossref_cursor(coll, url, data.message["next-cursor"]);
        }
      } else {
        await Save({ url: url, err: "URL error" }, "crossref", "error");
      }
    });
}

// (base_RIEPP, base, inColl, outColl)
export async function sjr_jurnal_works(outColl) {
  const bar1 = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );

  let issn = await GetBase("crossref", "issn");

  bar1.start(issn.length, 0);
  console.log("\n");

  let url_jurnal_works = "https://api.crossref.org/works?filter=";
  let end =
    ",from-pub-date:2017-01-01&mailto=kostukml@gmail.com&rows=1000&cursor=*";
  for (let i in issn) {
    let url =
      url_jurnal_works +
      issn[i].issn.reduce((pValue, cValue) => {
        return pValue == ""
          ? `${pValue}issn:${cValue}`
          : `${pValue},issn:${cValue}`;
      }, "") +
      end;
    try {
      await crossref_cursor(outColl, url, "");
    } catch (err) {
      await Update(
        { _id: issn[i]._id, issn: issn[i].issn, url: url, err: err },
        "crossref",
        "error"
      );
    }
    bar1.increment();
  }
  bar1.stop();
}

//убираем все ошибочные issn в которых 7 знаков
//должен получить 1395 новых запросов
export async function error_fix() {
  let inColl = "error";
  let err = await GetBase("crossref", inColl);
  for (let i in err) {
    let issn = err[i].url
      .split("issn:")
      .slice(1)
      .map((el) => {
        return {
          issn:
            el.split(",")[0].length != 8
              ? `${write_zeros(el.split(",")[0].length)}${el.split(",")[0]}`
              : el.split(",")[0],
        };
      });
    await Save(issn, "crossref", "issn_from_err");
  }
}

export async function match_doi_scopus_crossref(outColl) {
  const bar1 = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );

  let crossref = await GetBase("crossref", "sjr_works_2017", [
    {
      $project: {
        _id: "$_id",
      },
    },
  ]);

  bar1.start(crossref.length, 0);
  console.log("\n");
  let koll = 10000;
  for (let i = 0; i < crossref.length; i = i + koll) {
    let scopus = await GetBase_RIEPP("lab", "slw2022", [
      {
        $match: {
          doi: { $in: crossref.slice(i, i + koll).map((el) => el._id) },
        },
      },
    ]);
    await Update(scopus, "crossref", outColl);
    bar1.update(i + koll);
  }
  // for (let i in crossref) {
  //   let scopus = await GetBase_RIEPP("lab", "slw2022", [
  //     {
  //       $match: {
  //         doi: crossref[i]._id,
  //       },
  //     },
  //   ]);
  //   if (scopus.length != 0) {
  //     await Update(scopus, "crossref", outColl);
  //   } else {
  //     await Update(crossref[i], "crossref", "scopus_works_2017_empty");
  //   }
  //   bar1.increment();
  // }

  bar1.stop();
}

export async function count_fields() {
  async function count_f(data) {
    let match = {};
    match[data] = null;
    let aggr = [
      {
        $unwind: "$author",
      },
      {
        $match: match,
      },
      {
        $count: "koll",
      },
    ];
    return await GetBase("crossref", "sjr_works_2017", aggr);
  }
  let count = [
    `author.ORCID`,
    "DOI",
    "type",
    "created",
    "title",
    "container-title",
    "ISSN",
    "issn-type",
    "volume",
    "page",
    "journal-issue",
    "author",
    "publisher",
    "language",
    "subject",
    "reference",
    "abstract",
    "link",
  ];
  for (let i in count) {
    let koll = await count_f(count[i]);
    koll = koll.length == 0 ? 0 : koll[0].koll;
    console.log(`${count[i]}: ${koll}`);
  }
}

export async function create_issn_counter() {
  let aggregation = [
    {
      $lookup: {
        from: "scopus_works_2017",
        let: {
          issn: "$issn",
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $or: [
                  {
                    $in: ["$issn", "$$issn"],
                  },
                  {
                    $in: ["$eissn", "$$issn"],
                  },
                ],
              },
            },
          },
          {
            $count: "kol",
          },
        ],
        as: "res",
      },
    },
    {
      $unwind: "$res",
    },
    {
      $project: {
        issn: "$issn",
        kol: "$res.kol",
      },
    },
    {
      $out: "issn_counter",
    },
  ];
  await GetBase("crossref", "issn", aggregation);
}

export async function create_issn_counter_RIEEP() {
  let aggregation = [
    {
      $lookup: {
        from: "slw2022",
        let: {
          issn: "$issn",
          eissn: "$eissn",
        },
        pipeline: [
          { $match: { year: { $gte: "2017" } } },
          {
            $match: {
              $expr: {
                $or: [
                  {
                    $in: ["$issn", "$$issn"],
                  },
                  {
                    $in: ["$eissn", "$$issn"],
                  },
                ],
              },
            },
          },
          {
            $count: "kol",
          },
        ],
        as: "res",
      },
    },
    {
      $unwind: "$res",
    },
    {
      $project: {
        issn: "$issn",
        kol: "$res.kol",
      },
    },
    {
      $out: "issn_counter",
    },
  ];

  await GetBase_RIEPP("lab", "slw2022", aggregation);
}
