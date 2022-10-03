import { GetBase, Update } from "../db/utils";
import { create_normal_fio } from "./utils";

/**
 * 1) Создаем словарь афф из записей РНФ (func: AllOrgs)
 *
 * 1-2) Создать базу в которой к афф idscience (coll: allOrganizationsIdscience_with_authors)
 *      будет прикручены авторы idscience с их ринц версиями (coll: rinc-Idscience) (func:Idscience_org_and_authors)
 * 2) Работа с данными афф:
 *      - к OpenRIRO (coll: 1.basic) также привязваем их информацию о локации (coll: 2.geo) (func: OpenRIRO)
 *      - те связываем idscience (в которых уже есть информация о авторах idscience из этой организации(func: тут нет, но она простая))
 *        c OpenRIRO (coll: 1-2) связываем по inn (func: Idscience_to_OpenRIRO)
 *      -? может быть на этом этапе убрать массив найденных по inn с помощью ФИО и города
 *      - потом проводим связку афф РНФ-idscience (coll: AllOrgs и coll: Idscience_to_OpenRIRO)
 *        по ФИО разделяя на full, short, all (func: RNF_to_Idscience)
 * 3) Те в результате получаем базу в которой есть сопоставление афф по ФИО с idscience, сам idscience связан с OpenRIRO по inn
 *      и в афф idscience есть авторы
 * 4) Далее переходим к авторам
 *
 *
 */
/**
 * 1) Коллекции авторов:
 *    - РНФ в которой словарь авторов и список проектов (coll: allAuthors)
 *    - РИНЦ (coll: rinc_authors)
 *    - Idscience (coll: allAuthorsIdscience)
 *    Коллекции афф:
 *    - РНФ в которой словарь афф и список проектов (coll: allOrgs)
 *        у ["16-14-10044", "19-14-00134", "21-14-00226", "21-15-00169", "22-14-00174", "19-14-00134-П", "22-21-00930", "22-25-00602", "22-24-00729", "18-74-10059"]
 *        исправил ошибку с буквой "Академиии"->"Академии"
 *    - Idscience (coll: allOrganizationsIdscience)
 *    - OpenRIRO (coll: 1.basic и 2.geo)
 * 2) Операции с коллекциями:
 *    - к OpenRIRO (coll: 1.basic) привязваем информацию о локации (coll: 2.geo) (func: OpenRIRO)
 *    - проверяем (coll: allAuthorsIdscience) на то, что в их ссылке используется ринц id
 *      ЭТО ПРАВДА (у всех у кого есть ринц id, он совпадает с id в ссылке)
 *    - дополняем массив indexs обьект вида ниже и исправляем поле id в других системах (coll: allAuthorsIdscience_rinc_id) (func: Idscience_authors_rinc_id)
 *     {"name": "РИНЦ SPIN-код",
 *      "href": "https://elibrary.ru/author_profile.asp?id=/id из ссылки/",
 *      "id": /id из ссылки/}
 *    - привязываем к авторам и афф Idscience афф (coll: allAuthorsIdscience_with_affs)
 *      и авторов (coll: allOrganizationsIdscience_with_authors) соответсвенно (func: Idscience_authors_affs)
 * 3) Сопоставление OpenRIRO (coll: 1-2) и афф из Idscience по inn
 *    в которых есть список авторов (coll: allOrganizationsIdscience_with_authors)
 *    (coll: Idscience-OpenRIRO func: Idscience_to_OpenRIRO)
 * 3.1) Выбираю афф из главной или филиала (coll: Idscience-OpenRIRO) с помощью сначала full_name->город и head
 *      (coll: Idscience-OpenRIRO-choise func: Idscience_to_OpenRIRO_choice)
 * 4) Сопоставление профилей ринц (coll: rinc_authors) и Idscience (coll: allAuthorsIdscience_with_affs)
 *    по ринц id (coll: Idscience-rinc)
 * 4.1)???? добавить в Idsience-OpenRIRO ринц авторов
 * 5) Сопоставление профилей ринц (coll: rinc_authors) и РНФ (coll: allAuthors) по полному ФИО с учетом девичьих
 *    (coll: RNF-rinc func: RNF_to_rinc_authors)
 * 6) По полученным авторам (coll: RNF-rinc) дописываем (coll: Idscience-rinc) информацию из Idscience
 *    (coll: RNF-rinc-Idscience func: RNF_to_rinc_to_Idscience_authors)
 * 7) Сопоставление афф РНФ-idscience (coll: AllOrgs и coll: Idscience-OpenRIRO-choise)
 *    по ФИО разделяя на full, short (coll: RNF-Idscience-org func: RNF_to_Idscience)
 *    ----по short и по одному двойному full исправляем ручками
 *    и перезагружаем данные (coll: RNF-Idscience-org-correct func: RNF_to_Idscience_correct)
 * 
 * 
 * 
 *    -- дополнить веса для словарей авторов и афф в РНФ (это нужно делать скорее для пар автор афф)
 *    -- нужно сильнее посотреть на сопоставление афф рнф с Idscience

 */

export async function AllOrgs(base) {
  // создает словарь афф из РНФ и список их проектов
  // заменяет возможные английские буквы на русские
  // подправляет short_name
  let RNF = await GetBase(base, "RNF_project", [
    {
      $match: {
        Номер: { $ne: null },
      },
    },
    {
      $project: {
        _id: "$pusto",
        id: ["$_id"],
        name_full: "$Организация финансирования",
        name_short: "$org",
      },
    },
  ]);
  try {
    RNF = RNF.map((el) => {
      return {
        id: el.id,
        name_full: el.name_full
          .replace(/[«»]/gi, '"')
          .replace(/[M]/gi, "М")
          .replace(/[B]/gi, "В")
          .replace(/[C]/gi, "С")
          .replace(/[E]/gi, "Е")
          .replace(/[O]/gi, "О")
          .replace(/[X]/gi, "Х")
          .replace(/[P]/gi, "Р")
          .replace(/[H]/gi, "Н")
          .replace(/[K]/gi, "К")
          .replace(/[A]/gi, "А")
          .split(".")
          .map((el) => el.trim())
          .join(". ")
          .replace(/([А-Я]\.) ([А-Я]\.)/g, "$1$2"),
        name_short: el.name_short
          .replace(/[«»]/gi, '"')
          .replace(/[M]/gi, "М")
          .replace(/[B]/gi, "В")
          .replace(/[C]/gi, "С")
          .replace(/[E]/gi, "Е")
          .replace(/[O]/gi, "О")
          .replace(/[X]/gi, "Х")
          .replace(/[P]/gi, "Р")
          .replace(/[H]/gi, "Н")
          .replace(/[K]/gi, "К")
          .replace(/[A]/gi, "А")
          .split(".")
          .map((el) => el.trim())
          .join(". ")
          .replace(/([А-Я]\.) ([А-Я]\.)/g, "$1$2"),
      };
    });
    let data = [RNF.shift()];
    for (var i in RNF) {
      let index = data.findIndex(
        (el) =>
          el.name_full == RNF[i].name_full && el.name_short == RNF[i].name_short
      );
      if (index != -1) {
        data[index] = { ...data[index], id: data[index].id.concat(RNF[i].id) };
      } else {
        data.push(RNF[i]);
      }
    }
    await Update(data, base, "allOrgs");
  } catch (e) {
    console.error(e);
  }
}

export async function OpenRIRO(base) {
  // добавляет к 1.basic данные о геолокации из 2.geo по полю code
  let basic = await GetBase(base, "1.basic");
  let geo = await GetBase(base, "2.geo");
  try {
    let data = basic.map((el) => {
      //find можно тк так нет пересечений по code
      let geo_field = geo.find((elem) => elem.code == el.code);
      delete geo_field._id;
      delete geo_field.code;
      return { ...el, geo: geo_field };
    });
    await Update(data, base, "1-2");
  } catch (e) {
    console.error(e);
  }
}

export async function Idscience_authors_rinc_id(base) {
  let Idscience_authors = await GetBase(base, "allAuthorsIdscience");
  try {
    let data = Idscience_authors.map((el) => {
      return el.indexs.filter((elem) => elem.name == "РИНЦ SPIN-код").length ==
        0
        ? {
            ...el,
            indexs: el.indexs
              .map((elem) => {
                return {
                  ...elem,
                  id: elem.SPIN,
                };
              })
              .concat([
                {
                  name: "РИНЦ SPIN-код",
                  href: `https://elibrary.ru/author_profile.asp?id=${el.author.id}`,
                  id: el.author.id,
                },
              ]),
          }
        : {
            ...el,
            indexs: el.indexs.map((elem) => {
              return elem.name == "РИНЦ SPIN-код"
                ? elem
                : {
                    ...elem,
                    id: elem.SPIN,
                  };
            }),
          };
    });
    await Update(data, base, "allAuthorsIdscience_rinc_id");
  } catch (e) {
    console.error(e);
  }
}

export async function Idscience_authors_affs(base) {
  let Idscience_authors = await GetBase(base, "allAuthorsIdscience_rinc_id");
  let Idscience_affs = await GetBase(base, "allOrganizationsIdscience");

  try {
    let data_authors = Idscience_authors.map((el) => {
      return {
        ...el,
        affs: el.affs.map((elem) =>
          Idscience_affs.find((element) => element._id == elem.inn)
        ),
      };
    });
    let data_affs = Idscience_affs.map((el) => {
      return {
        ...el,
        authors: Idscience_authors.filter(
          (elem) => elem.affs.filter((element) => element.inn == el.inn) != 0
        ),
      };
    });
    await Update(data_authors, base, "allAuthorsIdscience_with_affs");
    await Update(data_affs, base, "allOrganizationsIdscience_with_authors");
  } catch (e) {
    console.error(e);
  }
}

export async function Idscience_to_OpenRIRO(base) {
  // находит афф из OpenRIRO по inn для записи idscience
  let Idscience = await GetBase(base, "allOrganizationsIdscience_with_authors");
  let OpenRIRO = await GetBase(base, "1-2");
  try {
    let data = Idscience.map((el) => {
      return {
        ...el,
        OpenRIRO: OpenRIRO.filter((elem) => el.inn == elem.inn),
      };
    });
    await Update(data, base, "Idscience-OpenRIRO");
  } catch (e) {
    console.error(e);
  }
}

export async function Idscience_to_OpenRIRO_choice(base) {
  //могу брать нулевой элемент массива тк сделал так что сопоставление 1в1
  let Idscience_Open_affs = await GetBase(base, "Idscience-OpenRIRO");
  for (let i in Idscience_Open_affs) {
    let data =
      Idscience_Open_affs[i].OpenRIRO.length > 1
        ? {
            ...Idscience_Open_affs[i],
            OpenRIRO2: Idscience_Open_affs[i].OpenRIRO.filter(
              (el) =>
                el.name_full.replace(/"/g, "").toLowerCase() ==
                  Idscience_Open_affs[i].full.replace(/"/g, "").toLowerCase() ||
                Idscience_Open_affs[i].full.replace(/"/g, "").toLowerCase() ==
                  `${el.opf_full.replace(/"/g, "").toLowerCase()} ${el.name_full
                    .replace(/"/g, "")
                    .toLowerCase()}`
            ),
          }
        : {
            ...Idscience_Open_affs[i],
            OpenRIRO2: Idscience_Open_affs[i].OpenRIRO,
          };
    data =
      data.OpenRIRO2.length == 0
        ? {
            ...data,
            OpenRIRO2: data.OpenRIRO.filter(
              (el) => el.geo.city == data.city && el.level == "Head"
            ),
          }
        : {
            ...data,
            OpenRIRO2: data.OpenRIRO2,
          };
    data.OpenRIRO = data.OpenRIRO2[0] ? data.OpenRIRO2[0] : {};
    delete data.OpenRIRO2;
    await Update(data, base, "Idscience-OpenRIRO-choise");
  }
}

export async function Idscience_rinc(base) {
  let Idscience_authors = await GetBase(base, "allAuthorsIdscience_with_affs");
  let rinc = await GetBase(base, "rinc_authors");
  try {
    rinc = rinc.reduce((pValue, cValue) => {
      pValue[cValue._id.slice(0, 1)]
        ? pValue[cValue._id.slice(0, 1)].push(cValue)
        : (pValue[cValue._id.slice(0, 1)] = [cValue]);
      return pValue;
    }, {});
    let data = Idscience_authors.map((el) => {
      let data1 = {
        ...el,
        rinc_authors: rinc[el.author.id.slice(0, 1)]
          ? rinc[el.author.id.slice(0, 1)].find(
              (elem) => elem._id == el.author.id
            )
          : {},
      };
      return {
        ...data1,
        rinc_authors: data1.rinc_authors ? data1.rinc_authors : {},
      };
    });
    await Update(data, base, "Idscience-rinc");
  } catch (e) {
    console.error(e);
  }
}

export async function RNF_to_rinc_authors(base) {
  let rinc = await GetBase(base, "rinc_authors");
  let rnf = await GetBase(base, "allAuthors");

  let name = "";
  let boys = "";
  let girls = "";
  let reg = /(.+) (\(.+\)) (.+)/g;

  rinc = rinc.reduce((pValue, cValue) => {
    let first_fio_letter = create_normal_fio(cValue.name).slice(0, 1);
    pValue[first_fio_letter]
      ? pValue[first_fio_letter].push(cValue)
      : (pValue[first_fio_letter] = [cValue]);
    return pValue;
  }, {});

  for (let i in rnf) {
    try {
      await Update(
        {
          ...rnf[i],
          rinc: rinc[create_normal_fio(rnf[i].full_name).slice(0, 1)]
            ? rinc[create_normal_fio(rnf[i].full_name).slice(0, 1)].filter(
                (elem) => {
                  boys = "";
                  girls = "";
                  name = elem.name.replace(/  /g, " ");
                  if (name.indexOf("(") != -1) {
                    boys = name.replace(reg, "$1 $3");
                    girls =
                      name
                        .replace(reg, "$2")
                        .substring(1, name.replace(reg, "$2").length - 1) +
                      " " +
                      name.replace(reg, "$3");
                  } else {
                    boys = name;
                  }
                  return (
                    create_normal_fio(boys) ==
                      create_normal_fio(rnf[i].full_name) ||
                    create_normal_fio(girls) ==
                      create_normal_fio(rnf[i].full_name)
                  );
                }
              )
            : [],
        },
        base,
        "RNF-rinc"
      );
    } catch (e) {
      console.error(e);
      console.log(rnf[i]);
    }
  }
}

export async function RNF_to_rinc_to_Idscience_authors(base) {
  let RNF_rinc = await GetBase(base, "RNF-rinc");
  let Idscience_authors = await GetBase(base, "Idscience-rinc");
  try {
    let data = RNF_rinc.map((el) => {
      return {
        ...el,
        rinc: el.rinc.map((elem) => {
          let data1 = Idscience_authors.find(
            (element) => element.rinc_authors._id == elem._id
          );
          return {
            ...elem,
            Idscience: data1
              ? {
                  _id: data1._id,
                  affs: data1.affs,
                  author: data1.author,
                  indexs: data1.indexs,
                }
              : {},
          };
        }),
      };
    });
    await Update(data, base, "RNF-rinc-Idscience");
  } catch (e) {
    console.error(e);
  }
}

function split_dot(text) {
  return text
    .replace(/["–\-]/g, "")
    .replace(/(\.[ а-яА-ЯёЁ])/g, (el) => {
      return `${el.slice(-1)}`;
    })
    .replace(/ +/g, "")
    .replace("ран", "российскойакадемиинаук");
}

export async function RNF_to_Idscience(base) {
  // сопоставление по name (full, short) афф РНФ и Idscience (вместе с инф из OpenRIRO)
  let RNF = await GetBase(base, "allOrgs");
  let Idscience = await GetBase(base, "Idscience-OpenRIRO-choise");

  try {
    let data = RNF.map((el) => {
      return {
        ...el,
        Idscience: {
          full:
            Idscience.filter(
              (elem) =>
                split_dot(elem.full.toLowerCase()) ==
                split_dot(el.name_full.toLowerCase())
            ).length == 0
              ? Idscience.filter(
                  (elem) =>
                    split_dot(elem.full.toLowerCase()).indexOf(
                      split_dot(el.name_full.toLowerCase())
                    ) != -1 ||
                    split_dot(el.name_full.toLowerCase()).indexOf(
                      split_dot(elem.full.toLowerCase())
                    ) != -1
                )
              : Idscience.filter(
                  (elem) =>
                    split_dot(elem.full.toLowerCase()) ==
                    split_dot(el.name_full.toLowerCase())
                ),
          short: Idscience.filter(
            (elem) =>
              split_dot(elem.short.toLowerCase()) ==
              split_dot(el.name_short.toLowerCase())
          ),
        },
      };
    });
    //по short и по одному двойному full исправляем ручками
    await Update(data, base, "RNF-Idscience-org");
  } catch (e) {
    console.error(e);
  }
}

export async function RNF_to_Idscience_correct(base) {
  let RNF_Idscience_org = await GetBase(base, "RNF-Idscience-org");
  try {
    let data = RNF_Idscience_org.map((el) => {
      return {
        ...el,
        Idscience: el.Idscience.full[0] ? el.Idscience.full[0] : {},
      };
    });
    await Update(data, base, "RNF-Idscience-org-correct");
  } catch (e) {
    console.error(e);
  }
}

export async function collect_res_RNF(base) {
  let authors = await GetBase(base, "RNF-rinc-Idscience");
  let orgs = await GetBase(base, "RNF-Idscience-org-correct");
  try {
    let data = [];
    for (let i in authors) {
      for (let j in authors[i].id) {
        let org = orgs.find((el) => el.id.indexOf(authors[i].id[j]) != -1);
        data.findIndex(
          (el) =>
            el.full_name == authors[i].full_name &&
            el.name_full == org.name_full &&
            el.name_short == org.name_short
        ) != -1
          ? data[
              data.findIndex(
                (el) =>
                  el.full_name == authors[i].full_name &&
                  el.name_full == org.name_full &&
                  el.name_short == org.name_short
              )
            ].id.push(authors[i].id[j])
          : data.push({
              _id: authors[i].id[j],
              id: [authors[i].id[j]],
              full_name: authors[i].full_name,
              name_full: org.name_full,
              name_short: org.name_short,
              rinc: authors[i].rinc,
              Idscience: org.Idscience,
            });
      }
    }
    await Update(data, base, "collect-res-RNF");
  } catch (e) {
    console.error(e);
  }
}

export async function res(base) {
  for (let j = 0; j < 10464; j++) {
    let res = await GetBase(
      base,
      "collect-res-RNF",
      [{ $skip: j }, { $limit: 1 }]
      //   { $match: { rinc: { $size: 2 } } },
      //   { $match: { "rinc.Idscience": { $ne: {} } } },
      //   { $limit: 10 },
      // ]
    );
    try {
      for (let i in res) {
        let r = [];
        if (res[i].rinc.length != 0 && res[i].Idscience._id) {
          r = res[i].rinc
            .filter((el) => el.Idscience.affs)
            .map((el) => {
              return el.Idscience.affs.findIndex(
                (elem) => elem._id == res[i].Idscience._id
              ) != -1
                ? el.Idscience.indexs
                : [];
            })
            .filter((el) => el.length != 0);
        }
        let data = { ...res[i], res: r };
        await Update(data, base, "collect-res-RNF");
      }
    } catch (e) {
      console.error(e);
    }
  }
}

export async function res_and_24id(base) {
  //для этого нужно еще сделать колл res_aut_all_letters(ё,й) и из нее id-exst_rinc-[]
  let res = await GetBase(base, "collect-res-RNF", [
    {
      $match: {
        rinc: [],
      },
    },
    {
      $lookup: {
        from: "id-exst_rinc-[]",
        localField: "full_name",
        foreignField: "full_name",
        as: "Idscience_authors",
      },
    },
    {
      $unwind: "$Idscience_authors",
    },
    {
      $addFields: {
        Idscience_authors: "$Idscience_authors.idscience",
      },
    },
  ]);
  try {
    for (let i in res) {
      if (res[i].Idscience_authors.length != 0 && res[i].Idscience._id) {
        res[i].res = res[i].Idscience_authors.filter((el) => el.affiliation)
          .map((el) => {
            return el.affiliation.findIndex(
              (elem) => elem._id == res[i].Idscience._id
            ) != -1
              ? el.indexs
              : [];
          })
          .filter((el) => el.length != 0);
      }
      await Update(res[i], base, "collect-res-RNF");
    }
  } catch (e) {
    console.error(e);
  }
}
