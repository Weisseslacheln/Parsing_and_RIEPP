import { Save, GetBase, Update } from "../db/utils";
import { create_normal_fio } from "./utils";

export async function MergeOrg(base, coll1, coll2, coll_out) {
  let RNF = await GetBase(base, coll1, [
    {
      $match: {
        Номер: { $ne: null },
      },
    },
    {
      $project: {
        _id: "$_id",
        name_full: "$Организация финансирования",
        name_short: "$org",
      },
    },
  ]);
  let OpenRIRO = await GetBase(base, coll2, [
    {
      $project: {
        _id: "$_id",
        code: "$code",
        name_full: "$name_full",
        name_short: "$name_short",
      },
    },
  ]);

  RNF = RNF.map((el) => {
    return {
      _id: el._id,
      name_full: el.name_full.replace(/[^a-zа-яё ]/gi, "").toLowerCase(),
      name_short: el.name_short.replace(/[^a-zа-яё ]/gi, "").toLowerCase(),
    };
  });
  OpenRIRO = OpenRIRO.map((el) => {
    return {
      _id: el._id,
      code: el.code,
      name_full: el.name_full.replace(/[^a-zа-яё ]/gi, "").toLowerCase(),
      name_short: el.name_short
        ? el.name_short
            .split(", ")
            .map((elem) => elem.replace(/[^a-zа-яё ]/gi, "").toLowerCase())
        : [""],
    };
  });

  let good_result = [];
  let bad_result = [];

  for (var i in RNF) {
    for (var j in OpenRIRO) {
      if (
        OpenRIRO[j].name_short.filter(
          (el) =>
            el == RNF[i].name_short ||
            ` ${el} `.indexOf(` ${RNF[i].name_short} `) != -1 ||
            (` ${RNF[i].name_short} `.indexOf(` ${el} `) != -1 && el != "")
        ).length != 0 ||
        OpenRIRO[j].name_full == RNF[i].name_full
      ) {
        good_result.push({
          RNF_id: RNF[i]._id,
          OpenRIRO_id: OpenRIRO[j]._id,
          OpenRIRO_code: OpenRIRO[j].code,
        });
        break;
      }
      if (j == OpenRIRO.length - 1) {
        bad_result.push({
          RNF_id: RNF[i]._id,
        });
      }
    }
  }
  await Save(good_result, base, `${coll_out}`);
  await Save(bad_result, base, `bad_${coll_out}`);
  /*
  проверить
  14-15-00197 627acf3a1f57f93e36911071
  14-19-01726 627acf3a1f57f93e36911629
  [
  {
    '$lookup': {
      'from': 'RNF_project', 
      'localField': 'RNF_id', 
      'foreignField': '_id', 
      'as': 'RNF'
    }
  }, {
    '$unwind': '$RNF'
  }, {
    '$lookup': {
      'from': '1.basic', 
      'localField': 'OpenRIRO_id', 
      'foreignField': '_id', 
      'as': 'OpenRIRO'
    }
  }, {
    '$unwind': '$OpenRIRO'
  }, {
    '$out': 'full_basic_merge'
  }
]

[
  {
    '$lookup': {
      'from': 'RNF_project', 
      'localField': 'RNF_id', 
      'foreignField': '_id', 
      'as': 'RNF'
    }
  }, {
    '$unwind': '$RNF'
  }, {
    '$out': 'full_bad_basic_merge'
  }
]
   */
  // забрать англ с рнф
  // сделать риест орг из моей потом сопоставить с той
  // https://idscience.ru/
  // сопоставить через этот сайт авторов,
  //https://rscf.ru/project/14-19-01652/
  //body > div.container.mt-5.mb-5 > div > div:nth-child(28) > span дописать возможность практического использования
}

export async function MergeOrg2(base, coll1, coll2, coll_out) {
  let RNF = await GetBase(base, coll1, [
    {
      $group: {
        _id: {
          org: "$org",
          full_org: "$full_org",
        },
        sum: {
          $sum: 1,
        },
      },
    },
    {
      $addFields: {
        name_full: "$_id.full_org",
        name_short: "$_id.org",
      },
    },
  ]);
  let OpenRIRO = await GetBase(base, coll2, [
    {
      $project: {
        _id: "$_id",
        code: "$code",
        name_full: "$name_full",
        name_short: "$name_short",
      },
    },
  ]);

  RNF = RNF.map((el) => {
    return {
      _id: el._id,
      sum: el.sum,
      name_full: el.name_full.replace(/[^a-zа-яё ]/gi, "").toLowerCase(),
      name_short: el.name_short.replace(/[^a-zа-яё ]/gi, "").toLowerCase(),
    };
  });
  OpenRIRO = OpenRIRO.map((el) => {
    return {
      _id: el._id,
      code: el.code,
      name_full_: el.name_full,
      name_short_: el.name_short,
      name_full: el.name_full.replace(/[^a-zа-яё ]/gi, "").toLowerCase(),
      name_short: el.name_short
        ? el.name_short
            .split(", ")
            .map((elem) => elem.replace(/[^a-zа-яё ]/gi, "").toLowerCase())
        : [""],
    };
  });

  let good_result = [];
  let bad_result = [];

  for (var i in RNF) {
    let mass_name_short = [];
    let mass_name_short_elRNF = [];
    let mass_name_short_RNFel = [];
    let mass_name_full = [];
    for (var j in OpenRIRO) {
      if (
        OpenRIRO[j].name_short.filter((el) => el == RNF[i].name_short).length !=
        0
      ) {
        mass_name_short.push({
          OpenRIRO_id: OpenRIRO[j]._id,
          OpenRIRO_code: OpenRIRO[j].code,
          OpenRIRO_name_full_: OpenRIRO[j].name_full_,
          OpenRIRO_name_short_: OpenRIRO[j].name_short_,
        });
      }
      if (
        OpenRIRO[j].name_short.filter(
          (el) => ` ${el} `.indexOf(` ${RNF[i].name_short} `) != -1
        ).length != 0
      ) {
        mass_name_short_elRNF.push({
          OpenRIRO_id: OpenRIRO[j]._id,
          OpenRIRO_code: OpenRIRO[j].code,
          OpenRIRO_name_full_: OpenRIRO[j].name_full_,
          OpenRIRO_name_short_: OpenRIRO[j].name_short_,
        });
      }
      if (
        OpenRIRO[j].name_short.filter(
          (el) => ` ${RNF[i].name_short} `.indexOf(` ${el} `) != -1 && el != ""
        ).length != 0
      ) {
        mass_name_short_RNFel.push({
          OpenRIRO_id: OpenRIRO[j]._id,
          OpenRIRO_code: OpenRIRO[j].code,
          OpenRIRO_name_full_: OpenRIRO[j].name_full_,
          OpenRIRO_name_short_: OpenRIRO[j].name_short_,
        });
      }
      if (OpenRIRO[j].name_full == RNF[i].name_full) {
        mass_name_full.push({
          OpenRIRO_id: OpenRIRO[j]._id,
          OpenRIRO_code: OpenRIRO[j].code,
          OpenRIRO_name_full_: OpenRIRO[j].name_full_,
          OpenRIRO_name_short_: OpenRIRO[j].name_short_,
        });
      }
    }
    if (
      mass_name_short.length == 0 &&
      mass_name_short_elRNF.length == 0 &&
      mass_name_short_RNFel.length == 0 &&
      mass_name_full.length == 0
    ) {
      bad_result.push({
        RNF_id: RNF[i]._id,
      });
    } else {
      good_result.push({
        RNF_id: RNF[i]._id,
        res: {
          mass_name_short: mass_name_short,
          mass_name_short_elRNF: mass_name_short_elRNF,
          mass_name_short_RNFel: mass_name_short_RNFel,
          mass_name_full: mass_name_full,
        },
      });
    }
  }
  await Save(good_result, base, `${coll_out}`);
  await Save(bad_result, base, `bad_${coll_out}`);
  /*
  проверить
  14-15-00197 627acf3a1f57f93e36911071
  14-19-01726 627acf3a1f57f93e36911629
  [
  {
    '$lookup': {
      'from': 'RNF_project', 
      'localField': 'RNF_id', 
      'foreignField': '_id', 
      'as': 'RNF'
    }
  }, {
    '$unwind': '$RNF'
  }, {
    '$lookup': {
      'from': '1.basic', 
      'localField': 'OpenRIRO_id', 
      'foreignField': '_id', 
      'as': 'OpenRIRO'
    }
  }, {
    '$unwind': '$OpenRIRO'
  }, {
    '$out': 'full_basic_merge'
  }
]

[
  {
    '$lookup': {
      'from': 'RNF_project', 
      'localField': 'RNF_id', 
      'foreignField': '_id', 
      'as': 'RNF'
    }
  }, {
    '$unwind': '$RNF'
  }, {
    '$out': 'full_bad_basic_merge'
  }
]
   */
  // забрать англ с рнф
  // сделать риест орг из моей потом сопоставить с той
  // https://idscience.ru/
  // сопоставить через этот сайт авторов,
  //https://rscf.ru/project/14-19-01652/
  //body > div.container.mt-5.mb-5 > div > div:nth-child(28) > span дописать возможность практического использования
}

export async function AllOrgs(base, inColl, outColl) {
  let RNF = await GetBase(base, inColl, [
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
  await Save(data, base, outColl);
}

export async function AllAuthors(base, inColl, outColl) {
  let RNF = await GetBase(base, inColl, [
    {
      $match: {
        Номер: { $ne: null },
      },
    },
    {
      $project: {
        _id: "$pusto",
        id: ["$_id"],
        full_name: "$Руководитель",
      },
    },
  ]);
  RNF = RNF.map((el) => {
    return {
      id: el.id,
      full_name: el.full_name.trim(),
      family: el.full_name.split(" ")[0].trim(),
    };
  });
  let data = [RNF.shift()];
  for (var i in RNF) {
    let index = data.findIndex((el) => el.full_name == RNF[i].full_name);
    if (index != -1) {
      data[index] = { ...data[index], id: data[index].id.concat(RNF[i].id) };
    } else {
      data.push(RNF[i]);
    }
  }
  await Save(data, base, outColl);
}

export async function allOrganizationsIdscience(base, inColl) {
  base = test;
  inColl = "allOrgsIdscience";
  let agg = [
    {
      $group: {
        _id: {
          short: "$short",
          full: "$full",
          city: "$city",
          href: "$href",
          inn: "$inn",
        },
      },
    },
    {
      $project: {
        _id: "$_id.inn",
        short: "$_id.short",
        full: "$_id.full",
        city: "$_id.city",
        href: "$_id.href",
        inn: "$_id.inn",
      },
    },
    { $out: "allOrganizationsIdscience" },
  ];
}

export async function ids_to_open_orgs(base, inColl) {
  // сопоставление по inn ids и open (orgs)
  base = test;
  inColl = "allOrganizationsIdscience";
  let agg = [
    {
      $lookup: {
        from: "1.basic",
        let: {
          inn1: "$inn",
        },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  {
                    $eq: ["$$inn1", "$inn"],
                  },
                  {
                    $eq: ["Head", "$level"],
                  },
                  {
                    $eq: ["Действующая организация", "$status"],
                  },
                ],
              },
            },
          },
        ],
        as: "openriro",
      },
    },
    { $out: "ids-to-open(orgs)" },
  ];
}

export async function ids_to_rnf_orgs(base, inColl1, inColl2, outColl) {
  let ids = await GetBase(base, inColl1, [
    {
      $match: {
        openriro: {
          $ne: [],
        },
      },
    },
  ]);
  let RNF = await GetBase(base, inColl2);

  let data = RNF.map((el) => {
    return {
      ...el,
      full: ids.filter(
        (elem) => elem.full.toLowerCase() == el.name_full.toLowerCase()
      ),
      short: ids.filter(
        (elem) => elem.short.toLowerCase() == el.name_short.toLowerCase()
      ),
      all: ids.filter(
        (elem) =>
          elem.full.toLowerCase() == el.name_full.toLowerCase() &&
          elem.short.toLowerCase() == el.name_short.toLowerCase()
      ),
    };
  });

  await Save(data, base, outColl);
}

export async function ids_to_rnf_auf(base, inColl1, inColl2, outColl) {
  let ids = await GetBase(base, inColl1);
  let RNF = await GetBase(base, inColl2);

  let data = RNF.map((el) => {
    return {
      ...el,
      full: ids.filter(
        (elem) => elem.author.name.toLowerCase() == el.full_name.toLowerCase()
      ),
    };
  });

  await Save(data, base, outColl);
}

export async function rinc_authors_to_rnf(base, inColl1, inColl2, outColl) {
  let rinc = await GetBase(base, inColl1);
  let rnf = await GetBase(base, inColl2);

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
        outColl
      );
    } catch (e) {
      console.error(e);
      console.log(rnf[i]);
    }
  }
}

export async function idscience_authors_to_rnf(
  base,
  inColl1,
  inColl2,
  outColl
) {
  let idscience = await GetBase(base, inColl1);
  let rnf = await GetBase(base, inColl2);

  idscience = idscience.reduce((pValue, cValue) => {
    let first_fio_letter = create_normal_fio(cValue.author.name).slice(0, 1);
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
          idscience: idscience[create_normal_fio(rnf[i].full_name).slice(0, 1)]
            ? idscience[create_normal_fio(rnf[i].full_name).slice(0, 1)].filter(
                (elem) =>
                  create_normal_fio(elem.author.name) ==
                  create_normal_fio(rnf[i].full_name)
              )
            : [],
        },
        base,
        outColl
      );
    } catch (e) {
      console.error(e);
      console.log(rnf[i]);
    }
  }
}

export async function allOrganizationsIdscience_with_authors(base, inColl) {
  let pipeline = [
    {
      $lookup: {
        from: "allAuthorsIdscience",
        let: {
          inn: "$inn",
        },
        pipeline: [
          {
            $unwind: "$affs",
          },
          {
            $match: {
              $expr: {
                $and: [
                  {
                    $eq: ["$affs.inn", "$$inn"],
                  },
                ],
              },
            },
          },
          {
            $addFields: {
              affs: "$sd",
            },
          },
        ],
        as: "authors",
      },
    },
    {
      $out: "allOrganizationsIdscience_with_authors",
    },
  ];
  await GetBase(base, inColl, pipeline);
}
export async function all_base_letters_ё_й(base, inColl) {
  let pipeline = [
    {
      $lookup: {
        from: "res_aut_rinc_letters(ё,й)",
        localField: "_id",
        foreignField: "_id",
        as: "rinc",
      },
    },
    {
      $unwind: "$rinc",
    },
    {
      $addFields: {
        rinc: "$rinc.rinc",
      },
    },
    {
      $out: "all_base_letters(ё,й)",
    },
  ];
  await GetBase(base, inColl, pipeline);
}
