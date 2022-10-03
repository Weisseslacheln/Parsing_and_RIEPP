import { GetBase, Update, GetBase_RIEPP, Update_RIEPP } from "../db/utils";

/**
 * 1) Скачиваем с РНФ аглиские версии проектов
 *    (coll: RNF_project_en_new func: в парсинге с en добавкой)
 * 2) Из полученных сопоставлений (coll: collect-res-RNF) выьираем те у которых нашелся Idscience и по нему OpenRIRO
 *    и с помощью (coll: 6.scopus) вытаскиваем их и добавляем скачанную (coll: RNF_project_en_new) англ информацию
 *    (coll: OpenRIRO-scopus_aff func: OpenRIRO_scopus_aff)
 * 3) Составляем базу из которой мы будет делать запрос к публикациям скопус из (coll: RNF_project_en_new) и (coll: OpenRIRO-scopus_aff)
 *    (coll:Preparation_for_scopus func:Preparation_for_scopus)
 * 4)
 *
 */

export async function OpenRIRO_scopus_aff(base) {
  let lookup = [
    {
      $match: {
        Idscience: {
          $ne: {},
        },
      },
    },
    {
      $match: {
        "Idscience.OpenRIRO": {
          $ne: {},
        },
      },
    },
    {
      $lookup: {
        from: "6.scopus",
        localField: "Idscience.OpenRIRO.code",
        foreignField: "code",
        as: "scopus",
      },
    },
    {
      $out: "OpenRIRO-scopus_aff",
    },
  ];

  await GetBase(base, "collect-res-RNF", lookup);
}

export async function Preparation_for_scopus(base) {
  let RNF_project_en = await GetBase(base, "RNF_project_en_new", [
    {
      $match: {
        Affiliation: {
          $ne: null,
        },
      },
    },
  ]);
  let OpenRIRO_scopus_aff = await GetBase(base, "OpenRIRO-scopus_aff", [
    {
      $project: {
        id: "$id",
        res: "$res",
        scopus: "$scopus",
      },
    },
  ]);

  try {
    let data = {};
    let project = {};
    for (let i in OpenRIRO_scopus_aff) {
      OpenRIRO_scopus_aff[i].id.map(async (el) => {
        project = RNF_project_en.find((elem) => elem._id == el);
        if (project != undefined) {
          data = {
            _id: project._id,
            Affiliation: project.Affiliation,
            "Implementation period": project["Implementation period"],
            "Project Lead": project["Project Lead"],
            materials: project.materials,
            scopus: OpenRIRO_scopus_aff[i].scopus,
            res: OpenRIRO_scopus_aff[i].res,
          };
          await Update(data, base, "Preparation_for_scopus");
        }
      });
    }
  } catch (e) {
    console.error(e);
  }
}

function lookup_Scopus_publ(scopus, materials, name) {
  return [
    {
      $match: {
        "affiliation.id": scopus.scopus_id,
        year: materials.year,
        "author.name": new RegExp(
          `${name.split(" ")[0]} ${name.split(" ")[1].slice(0, 1)}`
        ),
      },
    },
  ];
}

function scopus_res(scopus_publ, materials_title, scopus_name) {
  return scopus_publ
    .map((el, index) => {
      return materials_title.findIndex(
        (elem) =>
          elem == el.title.replace(/"/g, "").replace(/ /g, "").toLowerCase()
      ) != -1
        ? el.author
            .filter(
              (elem) =>
                elem.name.indexOf(
                  `${scopus_name.split(" ")[0]} ${scopus_name
                    .split(" ")[1]
                    .slice(0, 1)}`
                ) != -1
            )
            .map((elem) => {
              if (elem.length != 0)
                return {
                  ind: index,
                  ind_rnf: materials_title.findIndex(
                    (element) =>
                      element ==
                      el.title.replace(/"/g, "").replace(/ /g, "").toLowerCase()
                  ),
                  title: el.title,
                  author: elem,
                };
            })
        : [];
    })
    .filter((el) => el.length != 0)
    .map((el) => {
      return el.reduce((pValue, cValue) => {
        Object.keys(pValue).length == 0
          ? (pValue = { ...cValue, author: [cValue.author] })
          : pValue.author.push(cValue.author);
        return pValue;
      }, {});
    });
}

// scopus_all(Prepared[i],Prepared[i]["Project Lead"],materials,scopus_publ,base2)
async function scopus_all(Prepared, name, materials, scopus_publ, base2) {
  return await Promise.all(
    Prepared.scopus
      .filter((el) => el.scopus_affil_pubs)
      .sort((a, b) => b.scopus_affil_pubs - a.scopus_affil_pubs)
      .map(async (ell) => {
        return {
          [ell.scopus_id]: await Promise.all(
            materials.map(async (el) => {
              scopus_publ = await GetBase_RIEPP(
                base2,
                "slw2022",
                lookup_Scopus_publ(ell, el, name)
              );
              return {
                [el.year]: {
                  publ: scopus_publ,
                  res: scopus_res(
                    scopus_publ,
                    el.publ.map((elem) =>
                      elem.title
                        .replace(/"/g, "")
                        .replace(/ /g, "")
                        .toLowerCase()
                    ),
                    name
                  ),
                },
              };
            })
          ),
        };
      })
  );
}

export async function Scopus_publ(base1, base2) {
  let Prepared = await GetBase(base1, "Preparation_for_scopus", [
    {
      $match: {
        materials: {
          $ne: null,
        },
        scopus: { $ne: [] },
      },
    },
    {
      $match: {
        "materials.publ.year": {
          $in: [
            "2013",
            "2014",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
            "2022",
            "",
          ],
        },
      },
    },
  ]);

  let data = {};
  let scopus_publ = [];
  let materials = [];
  let reg = /(.+) (\(.+\)) (.+)/g;

  for (let i in Prepared) {
    try {
      materials = Prepared[i].materials.map((el) => {
        return {
          year: el.annotation.slice(-4),
          publ: [],
        };
      });

      Prepared[i].materials.map((el) => {
        materials = el.publ.reduce((pValue, cValue) => {
          delete cValue.num;
          let year_ind = pValue
            .map((elem) => elem.year)
            .findIndex(
              (elem) =>
                elem ==
                (cValue.year
                  ? cValue.year != ""
                    ? cValue.year
                    : el.annotation.slice(-4)
                  : el.annotation.slice(-4))
            );
          year_ind != -1
            ? pValue[year_ind].publ.push(cValue)
            : pValue.push({ year: cValue.year, publ: [cValue] });
          return pValue;
        }, materials);
      });

      if (Prepared[i]["Project Lead"].indexOf("(") == -1) {
        data = {
          ...Prepared[i],
          materials: materials,
          scopus_aff: Prepared[i].scopus,
          scopus: await scopus_all(
            Prepared[i],
            Prepared[i]["Project Lead"],
            materials,
            scopus_publ,
            base2
          ),
        };
      } else {
        data = {
          ...Prepared[i],
          materials: materials,
          scopus_aff: Prepared[i].scopus,
          scopus: await scopus_all(
            Prepared[i],
            `${Prepared[i]["Project Lead"].replace(reg, "$1")} ${Prepared[i][
              "Project Lead"
            ].replace(reg, "$3")}`,
            materials,
            scopus_publ,
            base2
          ),
          scopus_dev: await scopus_all(
            Prepared[i],
            `${Prepared[i]["Project Lead"]
              .replace(reg, "$2")
              .substring(
                1,
                Prepared[i]["Project Lead"].replace(reg, "$2").length - 1
              )} ${Prepared[i]["Project Lead"].replace(reg, "$3")}`,
            materials,
            scopus_publ,
            base2
          ),
        };
      }
      await Update(data, base1, "RNF-with-scopus_publ_all_years");
    } catch (e) {
      console.error(e);
    }
  }
}
/**
 * MongoServerError: BSONObj size: 16895013 (0x101CC25) is invalid. Size must be between 0 and 16793600(16MB) First element: update: "RNF-with-scopus_publ_all_years"
    at Connection.onMessage (C:\agri\parsing\node_modules\mongodb\lib\cmap\connection.js:203:30)
    at MessageStream.<anonymous> (C:\agri\parsing\node_modules\mongodb\lib\cmap\connection.js:63:60)
    at MessageStream.emit (events.js:315:20)
    at processIncomingData (C:\agri\parsing\node_modules\mongodb\lib\cmap\message_stream.js:108:16)
    at MessageStream._write (C:\agri\parsing\node_modules\mongodb\lib\cmap\message_stream.js:28:9)
    at writeOrBuffer (_stream_writable.js:352:12)
    at MessageStream.Writable.write (_stream_writable.js:303:10)
    at Socket.ondata (_stream_readable.js:719:22)
    at Socket.emit (events.js:315:20)
    at addChunk (_stream_readable.js:309:12) {
  ok: 0,
  code: 10334,
  codeName: 'BSONObjectTooLarge',
  [Symbol(errorLabels)]: Set(0) {}
}

RangeError [ERR_OUT_OF_RANGE]: The value of "offset" is out of range. It must be >= 0 && <= 17825792. Received 17825794
    at validateOffset (buffer.js:104:3)
    at Buffer.write (buffer.js:1055:5)
    at serializeString (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:25:18)
    at serializeInto (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:779:25)
    at serializeObject (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:280:20)
    at serializeInto (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:604:25)
    at serializeObject (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:280:20)
    at serializeInto (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:810:25)
    at serializeObject (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:280:20)
    at serializeInto (C:\agri\parsing\node_modules\bson\lib\parser\serializer.js:604:25) {
  code: 'ERR_OUT_OF_RANGE'
}
 */

export async function Scopus_publ_id(base) {
  try {
    let skip = 0;
    let Scopus = [0];
    while (Scopus.length != 0) {
      let Scopus = await GetBase(base, "RNF-with-scopus_publ_all_years", [
        // { $match: { _id: "18-12-00009" } },
        {
          $project: {
            _id: "$_id",
            scopus: "$scopus",
          },
        },
        { $skip: skip },
        { $limit: 100 },
      ]);
      for (let i in Scopus) {
        let scopus_res = [];
        Scopus[i].scopus.map((el) => {
          el[Object.keys(el)[0]].map((elem) => {
            if (elem[Object.keys(elem)[0]].res.length != 0) {
              elem[Object.keys(elem)[0]].res.map((element) => {
                element.author.map((document) => {
                  let index = scopus_res.findIndex(
                    (doc) => doc.id == document.id
                  );
                  index != -1
                    ? (scopus_res[index].affiliation = scopus_res[
                        index
                      ].affiliation.concat(document.affiliation))
                    : scopus_res.push(document);
                });
              });
            }
          });
        });
        scopus_res = scopus_res.map((el) => {
          let aff = el.affiliation.reduce((pValue, cValue) => {
            if (pValue.findIndex((doc) => doc.id == cValue.id) == -1) {
              pValue.push(cValue);
            }
            return pValue;
          }, []);
          return { ...el, affiliation: aff };
        });
        let data = {
          _id: Scopus[i]._id,
          scopus_res: scopus_res,
        };

        await Update(data, base, "RNF-with-scopus_publ_all_years");
      }
      skip += 100;
    }
  } catch (e) {
    console.error(e);
  }
}
