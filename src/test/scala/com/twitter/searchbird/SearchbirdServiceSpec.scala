package com.twitter.searchbird

class SearchbirdServiceSpec extends AbstractSpec {
  describe("SearchbirdService") {

    // TODO: Please implement your own tests.

    it("sets a key, then gets it") {
      searchbird.put("name", "bluebird")()
      assert(searchbird.get("name")() === "bluebird")
      intercept[Exception] { searchbird.get("what?")() }
    }
  }
}
