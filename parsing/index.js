import { startBrowser } from "./browser";
import {
  scrape,
  scrape_project,
  scrape_project_en,
  scrape_orgs,
  scrape_authors,
  scrape_Allauthors,
  scrape_Allorgs,
  scrape_CrossferJurnalList,
} from "./pageController";

export async function Parsing(base, coll) {
  let browserInstance = startBrowser();
  scrape(browserInstance, base, coll);
}

export async function GetProject(base, coll_in, coll_out) {
  let browserInstance = startBrowser();
  scrape_project(browserInstance, base, coll_in, coll_out);
}

export async function GetProject_en(base, coll_in, coll_out) {
  let browserInstance = startBrowser();
  scrape_project_en(browserInstance, base, coll_in, coll_out);
}

export async function GetOrgs(base, coll_in, coll_out) {
  let browserInstance = startBrowser();
  scrape_orgs(browserInstance, base, coll_in, coll_out);
}

export async function GetAuthors(base, coll_in, coll_out) {
  let browserInstance = startBrowser();
  scrape_authors(browserInstance, base, coll_in, coll_out);
}

export async function GetALLAuthors(base, coll_out) {
  let browserInstance = startBrowser();
  scrape_Allauthors(browserInstance, base, coll_out);
}

export async function GetALLOrgs(base, coll_out) {
  let browserInstance = startBrowser();
  scrape_Allorgs(browserInstance, base, coll_out);
}

export async function GetCrossferJurnalList(base, coll_out) {
  let browserInstance = startBrowser();
  scrape_CrossferJurnalList(browserInstance, base, coll_out);
}
