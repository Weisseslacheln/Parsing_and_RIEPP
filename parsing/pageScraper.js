import { ConnectionPoolClosedEvent } from "mongodb";
import { Save, GetBase, Update } from "../db/utils";
import fs from "fs";

const wait_till_HTML_rendered = async (page, timeout = 1000) => {
  const checkDurationMsecs = 1000;
  const maxChecks = timeout / checkDurationMsecs;
  let lastHTMLSize = 0;
  let checkCounts = 1;
  let countStableSizeIterations = 0;
  const minStableSizeIterations = 3;

  while (checkCounts++ <= maxChecks) {
    let html = await page.content();
    let currentHTMLSize = html.length;

    if (lastHTMLSize != 0 && currentHTMLSize == lastHTMLSize)
      countStableSizeIterations++;
    else countStableSizeIterations = 0;

    if (countStableSizeIterations >= minStableSizeIterations) {
      break;
    }

    lastHTMLSize = currentHTMLSize;
    await page.waitForTimeout(checkDurationMsecs);
  }
};

const auto_scroll = async (page, dist) => {
  await page.evaluate(async (scroll) => {
    await new Promise((resolve, reject) => {
      let totalHeight = 0;
      const distance = scroll;
      const timer = setInterval(() => {
        const scrollHeight = document.body.scrollHeight;
        window.scrollBy(0, distance);
        totalHeight += distance;

        if (totalHeight >= scrollHeight) {
          clearInterval(timer);
          resolve();
        }
      }, 0.01);
    });
  }, dist);
};

const next_page = async (page) => {
  await auto_scroll(
    page,
    await page.$$eval("#filtered-table > tbody > tr", (obj) => obj.length)
  );
  await page.click(`#search-projects-pagination > div.b-pagination-next > a`);
  await wait_till_HTML_rendered(page);
};

const removeDuplicates = (arr) => {
  //правильно работает для полей обьекта типа string и array (из обьекта с полями string)
  const result = [];
  const duplicatesIndices = [];

  arr.forEach((current, index) => {
    if (duplicatesIndices.includes(index)) return;

    result.push(current);

    for (
      let comparisonIndex = index + 1;
      comparisonIndex < arr.length;
      comparisonIndex++
    ) {
      const comparison = arr[comparisonIndex];
      const currentKeys = Object.keys(current);
      const comparisonKeys = Object.keys(comparison);

      if (currentKeys.length !== comparisonKeys.length) continue;

      const currentKeysString = currentKeys.sort().join("").toLowerCase();
      const comparisonKeysString = comparisonKeys.sort().join("").toLowerCase();
      if (currentKeysString !== comparisonKeysString) continue;

      let valuesEqual = true;
      for (let i = 0; i < currentKeys.length; i++) {
        const key = currentKeys[i];

        if (Array.isArray(current[key]) && Array.isArray(comparison[key])) {
          if (
            current[key].length !== comparison[key].length ||
            removeDuplicates(current[key]).length !==
              removeDuplicates(current[key].concat(comparison[key])).length ||
            removeDuplicates(comparison[key]).length !==
              removeDuplicates(current[key].concat(comparison[key])).length
          ) {
            valuesEqual = false;
            break;
          }
        } else {
          if (current[key] !== comparison[key]) {
            valuesEqual = false;
            break;
          }
        }
      }
      if (valuesEqual) duplicatesIndices.push(comparisonIndex);
    }
  });
  return result;
};

const scrape_current_page = async (page, projects) => {
  projects = projects.concat(
    await page.$$eval("#filtered-table > tbody > tr", (obj) => {
      obj = obj.map((el) => {
        var proj = {
          _id: el.querySelector("td:nth-child(2) > a").textContent,
          link: el.querySelector("td:nth-child(2) > a").href,
          code: [...el.querySelectorAll("td:nth-child(4) > span")].map(
            (code) => {
              return { title: code.title, id: code.textContent };
            }
          ),
          full_org: el.querySelector("td:nth-child(6) > span").title.trim(),
          org: el
            .querySelector("td:nth-child(6) > span")
            .textContent.replace(/[^a-zA-ZА-Яа-яЁё ]/gi, "")
            .trim(),
        };
        if (el.querySelector(" td:nth-child(3) > span:nth-child(4)"))
          proj.closed = el.querySelector(
            " td:nth-child(3) > span:nth-child(4)"
          ).textContent;
        return proj;
      });
      return obj;
    })
  );

  let nextButtonExist = await page.$eval(
    "#search-projects-pagination > div.b-pagination-next > a",
    (a) => {
      return a.style.display !== "none" ? true : false;
    }
  );

  if (nextButtonExist) {
    await next_page(page);
    return await scrape_current_page(page, projects);
  } else {
    return projects;
  }
};

const scrape_current_project = async (page) => {
  return {
    ...(await page.$$eval("body > div.container.mt-5.mb-5 > div > p", (obj) => {
      let publ = {};
      obj
        .map((el) => {
          return {
            name: el.querySelector("span.fld_title")
              ? el.querySelector("span.fld_title").textContent.trim()
              : "",
            text: el.textContent,
          };
        })
        .filter((el) => el.name !== "")
        .map((el) => {
          el.text = el.text
            .slice(el.text.indexOf(el.name) + el.name.length)
            .trim();
          if (el.name == "Руководитель") {
            publ[el.name] = el.text.split(",")[0];
            publ["Степень"] = el.text
              .split(",")[1]
              .split("\n")
              .join("")
              .split("\t")
              .join("")
              .trim();
          } else if (el.name == "Организация финансирования, регион") {
            publ["Организация финансирования"] =
              el.text
                .split(", ")
                .slice(0, -1)
                .join(", ")
                .charAt(0)
                .toUpperCase() +
              el.text.split(", ").slice(0, -1).join(", ").slice(1);
            publ["Регион"] = el.text.split(", ").pop();
          } else {
            publ[el.name] = el.text;
          }
        });
      return publ;
    })),
    ...(await page.$$eval(
      "body > div.container.mt-5.mb-5 > div > span > p",
      (obj) => {
        let publ = {};
        obj
          .map((el) => {
            return el.querySelector("a")
              ? {
                  name: el.querySelector("span.fld_title").textContent.trim(),
                  link: el.querySelector("a").href,
                  text: el.querySelector("a").textContent.trim(),
                }
              : {
                  name: el.querySelector("span.fld_title").textContent.trim(),
                  text: el.textContent,
                };
          })
          .map((el) => {
            if (el.link) {
              publ[el.name] = { text: el.text, link: el.link };
            } else {
              el.text = el.text
                .slice(el.text.indexOf(el.name) + el.name.length)
                .split("\n")
                .join("")
                .split("\t")
                .join("")
                .trim();
              if (el.name == "Область знания, основной код классификатора")
                publ[el.name] = el.text.split(/, (?!\W)/).map((elem) => {
                  return {
                    code: elem.split(" - ")[0],
                    text: elem.split(" - ")[1],
                  };
                });
              if (el.name == "Ключевые слова")
                publ[el.name] = el.text.split(", ");
              if (el.name == "Код ГРНТИ") publ[el.name] = el.text.split(" ");
              if (el.name == "Статус") publ[el.name] = el.text;
            }
          });
        return publ;
      }
    )),
    ...(await page.$$eval(
      "body > div.container.mt-5.mb-5 > div > table > tbody > tr",
      (obj) => {
        let publ = {};
        obj
          .map((el) => {
            return {
              name: el
                .querySelector("td:nth-child(1) > span.fld_title")
                .textContent.trim(),
              text: el
                .querySelector("td:nth-child(2) > span")
                .textContent.trim(),
              extended: el.querySelector("td:nth-child(3) > a")
                ? {
                    years: el
                      .querySelector("td:nth-child(3) > a")
                      .textContent.trim()
                      .replace(/, продлен на /g, "")
                      .slice(0, -1),
                    link: el.querySelector("td:nth-child(3) > a").href,
                  }
                : "",
            };
          })
          .map((el) => {
            if (el.extended) {
              publ[el.name] = { years: el.text, extended: el.extended };
            } else {
              publ[el.name] = { years: el.text };
            }
          });
        return publ;
      }
    )),
    materials: await page.$$eval(
      "body > div.container.mt-5.mb-5 > div > div",
      (obj) => {
        if (obj.length !== 0) {
          obj = obj.map((el) => {
            var proj = {
              annotation: el.querySelector("p:nth-child(1) > span.fld_title")
                .textContent,
              text: el.querySelector("p:nth-child(1) > span:nth-child(3)")
                .textContent,
              publ: [...el.querySelectorAll("div")].map((pub) => {
                return {
                  num: pub
                    .querySelector("p > span > b:nth-child(1)")
                    .textContent.trim(),
                  authors:
                    pub.querySelector("p > span > em").textContent.trim() == "-"
                      ? []
                      : pub
                          .querySelector("p > span > em")
                          .textContent.trim()
                          .split(", "),
                  title: pub
                    .querySelector("p > span > b:nth-child(3)")
                    .textContent.trim(),
                  info: pub
                    .querySelector("p > span")
                    .textContent.slice(
                      pub
                        .querySelector("p > span")
                        .textContent.indexOf(
                          pub
                            .querySelector("p > span > b:nth-child(3)")
                            .textContent.trim()
                        ) +
                        pub
                          .querySelector("p > span > b:nth-child(3)")
                          .textContent.trim().length
                    )
                    .split("(год публикации -")[0]
                    .trim(),
                  year: pub
                    .querySelector("p > span")
                    .textContent.slice(
                      pub
                        .querySelector("p > span")
                        .textContent.indexOf(
                          pub
                            .querySelector("p > span > b:nth-child(3)")
                            .textContent.trim()
                        ) +
                        pub
                          .querySelector("p > span > b:nth-child(3)")
                          .textContent.trim().length
                    )
                    .split("(год публикации -")[1]
                    .slice(0, -3)
                    .trim(),
                };
              }),
            };
            return proj;
          });
          return obj;
        }

        return undefined;
      }
    ),
  };
};

const scrape_current_project_en = async (page) => {
  return {
    ...(await page.$$eval("body > div.container.mt-5.mb-5 > div > p", (obj) => {
      let publ = {};
      obj
        .map((el) => {
          return {
            name: el.querySelector("span.fld_title")
              ? el.querySelector("span.fld_title").textContent.trim()
              : "",
            text: el.textContent,
          };
        })
        .filter((el) => el.name !== "")
        .map((el) => {
          el.text = el.text
            .slice(el.text.indexOf(el.name) + el.name.length)
            .trim();
          if (el.name == "Project Lead") {
            publ[el.name] = el.text.split(",")[0];
          } else if (el.name == "Affiliation") {
            publ["Affiliation"] = el.text;
          } else if (el.name == "Research area") {
            publ[el.name] = el.text.split(/, (?!\W)/).map((elem) => {
              return {
                code: elem.split(" - ")[0],
                text: elem.split(" - ")[1],
              };
            });
          } else if (el.name == "Keywords") {
            publ[el.name] = el.text.split(", ");
          } else {
            publ[el.name] = el.text;
          }
        });
      return publ;
    })),
    ...(await page.$$eval(
      "body > div.container.mt-5.mb-5 > div > table > tbody > tr",
      (obj) => {
        let publ = {};
        obj
          .map((el) => {
            return {
              name: el
                .querySelector("td:nth-child(1) > span.fld_title")
                .textContent.trim(),
              text: el
                .querySelector("td:nth-child(2) > span")
                .textContent.trim(),
              extended: el.querySelector("td:nth-child(3) > a")
                ? {
                    years: el
                      .querySelector("td:nth-child(3) > a")
                      .textContent.trim()
                      .replace(/extension for /g, "")
                      .slice(0, -1),
                    link: el.querySelector("td:nth-child(3) > a").href,
                  }
                : "",
            };
          })
          .map((el) => {
            if (el.extended) {
              publ[el.name] = { years: el.text, extended: el.extended };
            } else {
              publ[el.name] = { years: el.text };
            }
          });
        return publ;
      }
    )),
    materials: await page.$$eval(
      "body > div.container.mt-5.mb-5 > div > div",
      (obj) => {
        if (obj.length !== 0) {
          obj = obj.map((el) => {
            var proj = {
              annotation: el.querySelector("p:nth-child(1) > span.fld_title")
                .textContent,
              text: el.querySelector("p:nth-child(1) > span:nth-child(3)")
                .textContent,
              publ: [...el.querySelectorAll("div")].map((pub) => {
                return {
                  num: pub
                    .querySelector("p > span > b:nth-child(1)")
                    .textContent.trim(),
                  authors:
                    pub.querySelector("p > span > em").textContent.trim() == "-"
                      ? []
                      : pub
                          .querySelector("p > span > em")
                          .textContent.trim()
                          .split(", "),
                  title: pub
                    .querySelector("p > span > b:nth-child(3)")
                    .textContent.trim(),
                  info: pub
                    .querySelector("p > span")
                    .textContent.slice(
                      pub
                        .querySelector("p > span")
                        .textContent.indexOf(
                          pub
                            .querySelector("p > span > b:nth-child(3)")
                            .textContent.trim()
                        ) +
                        pub
                          .querySelector("p > span > b:nth-child(3)")
                          .textContent.trim().length
                    )
                    .split("(year -")[0]
                    .trim(),
                  year: pub
                    .querySelector("p > span")
                    .textContent.slice(
                      pub
                        .querySelector("p > span")
                        .textContent.indexOf(
                          pub
                            .querySelector("p > span > b:nth-child(3)")
                            .textContent.trim()
                        ) +
                        pub
                          .querySelector("p > span > b:nth-child(3)")
                          .textContent.trim().length
                    )
                    .split("(year -")[1]
                    .slice(0, -3)
                    .trim(),
                };
              }),
            };
            return proj;
          });
          return obj;
        }

        return undefined;
      }
    ),
  };
};

export const scraperRNF = {
  url: "https://rscf.ru/project/",
  async scraper(browser, base, coll) {
    let page = await browser.newPage();
    let allProject = [];

    await page.goto(this.url, { waitUntil: "networkidle0" });
    await wait_till_HTML_rendered(page);
    // вопрос о продлении проектов https://rscf.ru/project/14-14-00535/
    await page.click(
      "#search-projects-form > div.row > div:nth-child(1) > div > div > button"
    );
    let numberOfYears = await page.$$eval("#bs-select-1 > ul > li", (el) => {
      return [...el].length;
    });
    await page.click(
      "#search-projects-form > div.row > div:nth-child(1) > div > div > button"
    );
    //сейчас без <= те без 2023 года, тк там ничего нет
    for (let i = 2; i < numberOfYears; i++) {
      console.log(`#bs-select-1 > ul > li:nth-child(${i})`);
      await page.click(
        "#search-projects-form > div.row > div:nth-child(1) > div > div > button"
      );
      await page.click(`#bs-select-1 > ul > li:nth-child(${i})`);
      await page.click("#search-projects-btn");
      await wait_till_HTML_rendered(page);
      let scrapedData = await scrape_current_page(page, []);
      allProject = allProject.concat(removeDuplicates(scrapedData));
      console.log(allProject.length);
    }

    await Save(allProject, base, coll);
  },

  async scraper_project(browser, base, coll_in, coll_out) {
    let data = await GetBase(base, coll_in);
    let allProject = {};
    let page = await browser.newPage();
    for (var i = 0; i < data.length; i++) {
      try {
        await page.goto(data[i].link, { waitUntil: "networkidle0" });
        await wait_till_HTML_rendered(page);
        allProject = {
          ...data[i],
          ...(await scrape_current_project(page)),
        };
        await Save(allProject, base, coll_out);
      } catch (err) {
        console.log(
          `Error in scraperRNF.scraper_project with ${data[i].link}`,
          err
        );
      }
    }
  },

  async scraper_project_en(browser, base, coll_in, coll_out) {
    let data = await GetBase(base, coll_in, [
      {
        $lookup: {
          from: "RNF_project_en_new",
          localField: "_id",
          foreignField: "_id",
          as: "string",
        },
      },
      {
        $unwind: "$string",
      },
      {
        $match: {
          "string.Affiliation": null,
        },
      },
      {
        $addFields: {
          string: "$f",
        },
      },
    ]);
    let allProject = {};
    let page = await browser.newPage();
    for (var i = 0; i < data.length; i++) {
      try {
        await page.goto(
          `https://rscf.ru/en/project/${data[i].link.split("rid=")[1]}/`,
          {
            waitUntil: "networkidle0",
          }
        );
        await wait_till_HTML_rendered(page);
        allProject = {
          ...data[i],
          ...(await scrape_current_project_en(page)),
          link: `https://rscf.ru/en/project/${data[i].link.split("rid=")[1]}/`,
        };
        await Update(allProject, base, coll_out);
      } catch (err) {
        console.log(
          `Error in scraperRNF.scraper_project_en with ${data[i].link}`,
          err
        );
      }
    }
  },
};

const scrape_current_orgs = async (page, org) => {
  await page.type("#input-search", org);
  await page.click("#search-form > button");
  await wait_till_HTML_rendered(page);

  let inn = "";
  let res = await page.$$eval("#kt_datatable1_info", (obj) => {
    let doc = "";
    obj.map((el) => (doc = el.textContent));
    return doc;
  });

  if (res == "1 из 1") {
    inn = await page.$$eval("#kt_datatable1 > tbody", (obj) => {
      let doc = "";
      obj.map(
        (el) =>
          (doc = el.querySelector(
            "tr > td > div > div:nth-child(4) > a"
          ).textContent)
      );
      return doc;
    });
  } else {
    inn = res;
  }

  await page.focus("#input-search");
  await page.keyboard.down("Control");
  await page.keyboard.press("A");
  await page.keyboard.up("Control");
  await page.keyboard.press("Backspace");

  return inn;
};

const scrape_current_authors = async (page, author) => {
  await page.type("#input-search", author);
  await page.click(
    "#search-form > button.btn.btn-dark.font-weight-bold.btn-hover-light-primary.px-7.search"
  );
  await wait_till_HTML_rendered(page);

  let index = [];
  let orgs = [];
  let res = await page.$$eval("#kt_datatable_info", (obj) => {
    let doc = [];
    obj.map((el) => (doc = el.textContent));
    return doc;
  });
  if (res == "1 из 1") {
    index = await page.$$eval("#kt_datatable > tbody", (obj) => {
      let doc = [];
      obj.map((el) => {
        doc = [...el.querySelectorAll("tr > td:nth-child(4) > a")].map(
          (code) => {
            return { name: code.title, id: code.textContent };
          }
        );
      });
      return doc;
    });
    orgs = await page.$$eval("#kt_datatable > tbody", (obj) => {
      let doc = [];
      obj.map((el) => {
        doc = [...el.querySelectorAll("tr > td:nth-child(3) > a")].map(
          (code) => {
            return { name: code.textContent, href: code.href };
          }
        );
      });
      return doc;
    });
  } else {
    index = res == "" ? [] : [res];
  }

  await page.focus("#input-search");
  await page.keyboard.down("Control");
  await page.keyboard.press("A");
  await page.keyboard.up("Control");
  await page.keyboard.press("Backspace");
  await page.click(
    "#search-form > button.btn.btn-dark.font-weight-bold.btn-hover-light-primary.px-7.search"
  );
  await wait_till_HTML_rendered(page);

  return { index, orgs };
};

const next_page_idscience_au = async (page) => {
  await auto_scroll(
    page,
    await page.$$eval("#kt_datatable > tbody > tr", (obj) => obj.length)
  );
  await page.click(`#kt_datatable_next > a`);
  await wait_till_HTML_rendered(page);
};

const scrape_page_idscience_au = async (page, data) => {
  if (
    (await page.$$(`#kt_datatable > tbody > tr > td[class="dataTables_empty"]`))
      .length == 0
  ) {
    let mass_authors = await page.$$eval("#kt_datatable > tbody", (obj) => {
      let doc = [];
      obj.map((el) => {
        doc = [...el.querySelectorAll("tr")].map((code) => {
          return {
            author: {
              name: code.querySelector("td.dtr-control > div > a").title.trim(),
              href: code.querySelector("td.dtr-control > div > a").href.trim(),
              id: code
                .querySelector("td.dtr-control > div > a")
                .href.trim()
                .split("/?")[
                code.querySelector("td.dtr-control > div > a").href.split("/?")
                  .length - 1
              ],
            },
            affs: [...code.querySelectorAll("td:nth-child(3) > a")].map(
              (aff) => {
                return {
                  name: aff.textContent.trim(),
                  href: aff.href.trim(),
                  inn:
                    aff.href.trim().split("/").slice(-1) != ""
                      ? aff.href.trim().split("/").slice(-1)[0]
                      : aff.href
                          .trim()
                          .split("/")
                          .slice(-3)
                          .reduce((pValue, cValue) =>
                            cValue != "" ? `${pValue}/${cValue}` : pValue
                          ),
                };
              }
            ),
            indexs: [...code.querySelectorAll(" td:nth-child(4) > a")].map(
              (index) => {
                return {
                  name: index.title.trim(),
                  SPIN: index.textContent.trim(),
                  href: index.href.trim(),
                  id: index.href.trim().split("id=")[
                    index.href.split("id=").length - 1
                  ],
                };
              }
            ),
          };
        });
      });
      return doc;
    });

    const nextButtonExist =
      (
        await page.$$(
          `#kt_datatable_next[class="paginate_button page-item next disabled"]`
        )
      ).length == 0;

    if (nextButtonExist) {
      await next_page_idscience_au(page);
      return await scrape_page_idscience_au(page, data.concat(mass_authors));
    } else {
      return data.concat(mass_authors);
    }
  } else {
    return [];
  }
};

const next_page_idscience_org = async (page) => {
  await auto_scroll(
    page,
    await page.$$eval("#kt_datatable1 > tbody > tr", (obj) => obj.length)
  );
  await page.click(`#kt_datatable1_next > a`);
  await wait_till_HTML_rendered(page);
};

const scrape_page_idscience_org = async (page, data) => {
  if (
    (
      await page.$$(
        `#kt_datatable1 > tbody > tr > td[class="dataTables_empty"]`
      )
    ).length == 0
  ) {
    let mass_authors = await page.$$eval("#kt_datatable1 > tbody", (obj) => {
      let doc = [];
      obj.map((el) => {
        doc = [...el.querySelectorAll("tr")].map((code) => {
          return {
            short: code
              .querySelector("td > div > div:nth-child(1) > strong > a")
              .textContent.trim(),
            full: code.querySelector("td > div > div.sm").textContent.trim(),
            city: code
              .querySelector("td > div > div.opacity-70")
              .textContent.trim(),
            href: code
              .querySelector("td > div > div:nth-child(4) > a")
              .href.trim(),
            inn: code
              .querySelector("td > div > div:nth-child(4) > a")
              .textContent.trim(),
          };
        });
      });
      return doc;
    });

    const nextButtonExist =
      (
        await page.$$(
          `#kt_datatable1_next[class="paginate_button page-item next disabled"]`
        )
      ).length == 0;

    if (nextButtonExist) {
      await next_page_idscience_org(page);
      return await scrape_page_idscience_org(page, data.concat(mass_authors));
    } else {
      return data.concat(mass_authors);
    }
  } else {
    return [];
  }
};

export const scraperIdscience = {
  url_org: "https://idscience.ru/organizations/",
  url_author: "https://idscience.ru/researchers/",
  async scraper_orgs(browser, base, coll_in, coll_out) {
    let data = await GetBase(base, coll_in);
    let page = await browser.newPage();
    let allOrgs = {};

    await page.goto(this.url_org, { waitUntil: "networkidle0" });
    await wait_till_HTML_rendered(page);

    for (var i = 0; i < data.length; i++) {
      try {
        let inn = await scrape_current_orgs(page, data[i].name_full);
        if (inn == "") {
          inn = await scrape_current_orgs(page, data[i].name_short);
        }
        allOrgs = {
          ...data[i],
          inn: inn,
        };
        await Save(allOrgs, base, coll_out);
      } catch (err) {
        console.log(
          `Error in scraperIdscience.scraper_orgs with ${data[i]._id}`,
          err
        );
      }
    }
  },
  async scraper_authors(browser, base, coll_in, coll_out) {
    let data = await GetBase(base, coll_in);
    let page = await browser.newPage();
    let allAuthors = {};

    await page.goto(this.url_author, { waitUntil: "networkidle0" });
    await wait_till_HTML_rendered(page);

    for (var i = 0; i < data.length; i++) {
      try {
        let res = await scrape_current_authors(page, data[i].full_name);
        // if (res.index.length == 0) {
        //   res = await scrape_current_authors(page, data[i].family);
        // }
        allAuthors = {
          ...data[i],
          ...res,
        };
        await Save(allAuthors, base, coll_out);
      } catch (err) {
        console.log(
          `Error in scraperIdscience.scraper_authors with ${data[i]._id}`,
          err
        );
      }
    }
  },
  async scraper_orgs_data(browser, base, coll_out) {
    let page = await browser.newPage();

    await page.goto(this.url_org, { waitUntil: "networkidle0" });
    await wait_till_HTML_rendered(page);

    try {
      let data = [];
      let letters = [
        "а",
        "б",
        "в",
        "г",
        "д",
        "е",
        "ё",
        "ж",
        "з",
        "и",
        "й",
        "к",
        "л",
        "м",
        "н",
        "о",
        "п",
        "р",
        "с",
        "т",
        "у",
        "ф",
        "х",
        "ц",
        "ч",
        "ш",
        "щ",
        "ь",
        "ы",
        "ъ",
        "э",
        "ю",
        "я",
      ];
      for (let i in letters) {
        await page.type("#input-search", letters[i]);
        await page.click("#search-form > button");
        await wait_till_HTML_rendered(page);

        await auto_scroll(
          page,
          await page.$$eval("#kt_datatable1 > tbody > tr", (obj) => obj.length)
        );
        await page.select("#kt_datatable1_length > label > select", "100");
        await wait_till_HTML_rendered(page);

        data = await scrape_page_idscience_org(page, []);
        if (data.length != 0) await Save(data, base, coll_out);

        await page.focus("#input-search");
        await page.keyboard.down("Control");
        await page.keyboard.press("A");
        await page.keyboard.up("Control");
        await page.keyboard.press("Backspace");
        await page.click("#search-form > button");
        await wait_till_HTML_rendered(page);
        console.log(`done with ${letters[i]}`);
      }
    } catch (err) {
      console.log(`Error in scraperIdscience.scraper_authors_data`, err);
    }
  },
  async scraper_authors_data(browser, base, coll_out) {
    let page = await browser.newPage();

    await page.goto(this.url_author, { waitUntil: "networkidle0" });
    await wait_till_HTML_rendered(page);

    try {
      let data = [];
      let letters = [
        "а",
        "б",
        "в",
        "г",
        "д",
        "е",
        "ё",
        "ж",
        "з",
        "и",
        "й",
        "к",
        "л",
        "м",
        "н",
        "о",
        "п",
        "р",
        "с",
        "т",
        "у",
        "ф",
        "х",
        "ц",
        "ч",
        "ш",
        "щ",
        "ь",
        "ы",
        "ъ",
        "э",
        "ю",
        "я",
      ];
      for (let i in letters) {
        await page.type("#input-search", letters[i]);
        await page.click(
          "#search-form > button.btn.btn-dark.font-weight-bold.btn-hover-light-primary.px-7.search"
        );
        await wait_till_HTML_rendered(page);

        await auto_scroll(
          page,
          await page.$$eval("#kt_datatable > tbody > tr", (obj) => obj.length)
        );
        await page.select("#kt_datatable_length > label > select", "100");
        await wait_till_HTML_rendered(page);

        data = await scrape_page_idscience_au(page, []);
        if (data.length != 0) await Save(data, base, coll_out);

        await page.focus("#input-search");
        await page.keyboard.down("Control");
        await page.keyboard.press("A");
        await page.keyboard.up("Control");
        await page.keyboard.press("Backspace");
        await page.click(
          "#search-form > button.btn.btn-dark.font-weight-bold.btn-hover-light-primary.px-7.search"
        );
        await wait_till_HTML_rendered(page);
        console.log(`done with ${letters[i]}`);
      }
    } catch (err) {
      console.log(`Error in scraperIdscience.scraper_authors_data`, err);
    }
  },
};

export const scraperCrossref = {
  url_jurnal: "http://data.crossref.org/depositorreport?pubid=J307799",
  async jurnal_list(browser, base, coll_out) {
    let page = await browser.newPage();

    await page.goto(this.url_jurnal, { waitUntil: "networkidle0" });
    await wait_till_HTML_rendered(page);

    try {
      let data = await page.evaluate(() => {
        let info = document.querySelector("body > pre").textContent;
        let mass = info.split("\n");
        let keys = mass[0].split(/ +/);

        return mass
          .slice(2)
          .map((el) => {
            let str = el.split(/ +/);
            let res = { _id: str[0] };
            for (let i = 0; i < str.length; i++) {
              if (str[i] != "") res = { ...res, [keys[i]]: str[i] };
            }
            return res;
          })
          .filter((el) => el._id != "");
      });

      await Update(data, base, coll_out);
    } catch (err) {
      console.log(`Error in scraperIdscience.scraper_authors_data`, err);
    }
  },
};
