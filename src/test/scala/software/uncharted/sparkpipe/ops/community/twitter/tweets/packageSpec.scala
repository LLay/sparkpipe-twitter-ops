/*
 * Copyright 2016 Uncharted Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.uncharted.sparkpipe.ops.community.twitter.tweets

import software.uncharted.sparkpipe.{Pipe, ops}
import software.uncharted.sparkpipe.ops.community.twitter.{Spark, Schemas}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest._

class PackageSpec extends FunSpec {
  describe("ops.community.twitter.tweets") {

    val path = "src/test/resources/sample-tweets.json"
    val format = "json"

    describe("#read()") {
      it("should pass arguments to the underlying sparkpipe.ops.core.dataframe.io.read() API") {
        val df = ops.community.twitter.tweets.read(path, format)(Spark.sqlContext)
        assert(df.count == 15)
      }
    }

    describe("TWEET_SCHEMA") {
      it("should be a subset of the schema") {
        val pipe = Pipe(Spark.sqlContext).to(ops.core.dataframe.io.read(path, format))
        assert(Schemas.subset(pipe.run.schema, TWEET_SCHEMA))
      }
    }

    describe("#extractHashtags()") {
      it("should create a new column of hashtags present in the tweet text") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(extractHashtags())
        val df = pipe.run
        val actual = df.select("hashtags").collect
        val desired = Array(Seq("WeLoveErdo\u011fan"), Seq(), Seq("TTIP", "TPP"), Seq(), Seq(), Seq(), Seq(), Seq(), Seq("Obama"), Seq(), Seq(), Seq("Obama", "nuclear"), Seq(), Seq(), Seq())

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }

    describe("#extractMentions()") {
      it("should create a new column of user mentions present in the tweet text") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(extractMentions())
        val df = pipe.run
        val actual = df.select("mentions").collect
        val desired = Array(Seq("seZen__333"), Seq("AbdulkerimYakut"), Seq("Matthijs85"), Seq("PoliticaDivan"), Seq(), Seq("TPM"), Seq(), Seq("DragonflyJonez"), Seq(), Seq("steph93065"), Seq("bootymath"), Seq("mattspetalnick", "mattspetalnick", "davidbrunnstrom"), Seq("heavenlyitalian"), Seq(), Seq("AP"))

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }

    describe("#extractURLs()") {
      it("should create a new column of all URLs present in the tweet object") {
        val pipe = Pipe(Spark.sqlContext).to(read(path, format)).to(extractURLs("urls"))
        val df = pipe.run
        val actual = df.select("urls").collect
        val desired = Array(
          Seq("http://abs.twimg.com/images/themes/theme1/bg.png", "https://abs.twimg.com/images/themes/theme1/bg.png", "https://pbs.twimg.com/profile_banners/3086536346/1455376807", "http://pbs.twimg.com/profile_images/705848768789422080/19s92luk_normal.jpg", "https://pbs.twimg.com/profile_images/705848768789422080/19s92luk_normal.jpg"),
          Seq("https://t.co/iY65kStcR1", "pic.twitter.com/iY65kStcR1", "http://twitter.com/AbdulkerimYakut/status/715098038600134656/photo/1", "http://pbs.twimg.com/media/CeyJ3u1W8AA8-52.jpg", "https://pbs.twimg.com/media/CeyJ3u1W8AA8-52.jpg", "http://abs.twimg.com/images/themes/theme1/bg.png", "https://abs.twimg.com/images/themes/theme1/bg.png", "https://pbs.twimg.com/profile_banners/2200856111/1419699191", "http://pbs.twimg.com/profile_images/666195619564937216/LnWYIQ_k_normal.jpg", "https://pbs.twimg.com/profile_images/666195619564937216/LnWYIQ_k_normal.jpg"),
          Seq("https://t.co/PpCh4mZbP6", "pic.twitter.com/PpCh4mZbP6", "http://twitter.com/Matthijs85/status/715195150876606465/photo/1", "http://pbs.twimg.com/media/CehhewcW8AEZxKT.jpg", "https://pbs.twimg.com/media/CehhewcW8AEZxKT.jpg", "thehill.com/blogs/congress\u2026", "http://thehill.com/blogs/congress-blog/foreign-policy/271678-last-chance-for-a-pro-transparency-trade-legacy-for-obama", "https://t.co/YZRRENLLpr", "https://pbs.twimg.com/profile_banners/695697958357094401/1454712418", "http://pbs.twimg.com/profile_images/695699403303047168/HTECke2F_normal.jpg", "https://pbs.twimg.com/profile_images/695699403303047168/HTECke2F_normal.jpg"),
          Seq("https://t.co/jwg0AkJQfn", "pic.twitter.com/jwg0AkJQfn", "http://twitter.com/PoliticaDivan/status/715194640518017025/video/1", "http://pbs.twimg.com/ext_tw_video_thumb/715193926009884674/pu/img/Pan_aLFY0dsQ-3mp.jpg", "https://pbs.twimg.com/ext_tw_video_thumb/715193926009884674/pu/img/Pan_aLFY0dsQ-3mp.jpg", "http://abs.twimg.com/images/themes/theme1/bg.png", "https://abs.twimg.com/images/themes/theme1/bg.png", "https://pbs.twimg.com/profile_banners/113422695/1455915691", "http://pbs.twimg.com/profile_images/700810715632898049/XK9U6fbb_normal.jpg", "https://pbs.twimg.com/profile_images/700810715632898049/XK9U6fbb_normal.jpg"),
          Seq("http://abs.twimg.com/images/themes/theme1/bg.png", "https://abs.twimg.com/images/themes/theme1/bg.png", "https://pbs.twimg.com/profile_banners/326951030/1458707300", "http://pbs.twimg.com/profile_images/699143141257940992/XU0s_Ops_normal.jpg", "https://pbs.twimg.com/profile_images/699143141257940992/XU0s_Ops_normal.jpg"),
          Seq("https://t.co/sHegapNYuf", "pic.twitter.com/sHegapNYuf", "http://twitter.com/TPM/status/715196762366017536/photo/1", "http://pbs.twimg.com/media/CezjqoDXEAAqq88.jpg", "https://pbs.twimg.com/media/CezjqoDXEAAqq88.jpg", "bit.ly/1M0iEDf", "http://bit.ly/1M0iEDf", "https://t.co/HJ6K0gsZwL", "dirk2112.tumblr.com", "http://dirk2112.tumblr.com/", "https://t.co/y1MUwCGWE6", "http://pbs.twimg.com/profile_background_images/85914090/forbidden_planet_poster_08.jpg", "https://pbs.twimg.com/profile_background_images/85914090/forbidden_planet_poster_08.jpg", "https://pbs.twimg.com/profile_banners/13745962/1349188368", "http://pbs.twimg.com/profile_images/610519957533773825/FMKnobMX_normal.jpg", "https://pbs.twimg.com/profile_images/610519957533773825/FMKnobMX_normal.jpg", "https://t.co/y1MUwCGWE6"),
          Seq("http://abs.twimg.com/images/themes/theme5/bg.gif", "https://abs.twimg.com/images/themes/theme5/bg.gif", "https://pbs.twimg.com/profile_banners/2532781354/1459329282", "http://pbs.twimg.com/profile_images/709914877599547392/qMPzdc30_normal.jpg", "https://pbs.twimg.com/profile_images/709914877599547392/qMPzdc30_normal.jpg"),
          Seq("traktrain.com/elryjtsn", "http://traktrain.com/elryjtsn", "https://t.co/UXo769tAbj", "http://pbs.twimg.com/profile_background_images/429187313/jean_michel_basquiat1024.jpg", "https://pbs.twimg.com/profile_background_images/429187313/jean_michel_basquiat1024.jpg", "https://pbs.twimg.com/profile_banners/331807772/1458129201", "http://pbs.twimg.com/profile_images/706616446323924992/GNEe6_UW_normal.jpg", "https://pbs.twimg.com/profile_images/706616446323924992/GNEe6_UW_normal.jpg", "https://t.co/UXo769tAbj"),
          Seq("themerylhancilesglobal.co.uk", "http://www.themerylhancilesglobal.co.uk", "https://t.co/7LYtFD5GjE", "http://abs.twimg.com/images/themes/theme1/bg.png", "https://abs.twimg.com/images/themes/theme1/bg.png", "https://pbs.twimg.com/profile_banners/701440866926379009/1456073866", "http://pbs.twimg.com/profile_images/701442247854833664/lWJQN63m_normal.jpg", "https://pbs.twimg.com/profile_images/701442247854833664/lWJQN63m_normal.jpg", "https://t.co/7LYtFD5GjE"),
          Seq("http://pbs.twimg.com/profile_images/710648534815666177/57eQBxYM_normal.jpg", "https://pbs.twimg.com/profile_images/710648534815666177/57eQBxYM_normal.jpg"),
          Seq("http://pbs.twimg.com/profile_background_images/882945483/2c40c351e75f21f2ac008fc5f569749b.jpeg", "https://pbs.twimg.com/profile_background_images/882945483/2c40c351e75f21f2ac008fc5f569749b.jpeg", "https://pbs.twimg.com/profile_banners/323576713/1458524763", "http://pbs.twimg.com/profile_images/712058343158915072/5hOWRReJ_normal.jpg", "https://pbs.twimg.com/profile_images/712058343158915072/5hOWRReJ_normal.jpg"),
          Seq("reuters.com/article/idUSKC\u2026", "http://www.reuters.com/article/idUSKCN0WW1R2", "https://t.co/KrYpVapTp7", "http://abs.twimg.com/images/themes/theme4/bg.gif", "https://abs.twimg.com/images/themes/theme4/bg.gif", "https://pbs.twimg.com/profile_banners/14375047/1398274926", "http://pbs.twimg.com/profile_images/476401132948815874/rY_WNywd_normal.jpeg", "https://pbs.twimg.com/profile_images/476401132948815874/rY_WNywd_normal.jpeg"),
          Seq("https://t.co/yRYeCSSQk8", "pic.twitter.com/yRYeCSSQk8", "http://twitter.com/heavenlyitalian/status/715190742692577280/photo/1", "http://pbs.twimg.com/media/CezeLgHWIAA6LYZ.jpg", "https://pbs.twimg.com/media/CezeLgHWIAA6LYZ.jpg", "http://abs.twimg.com/images/themes/theme1/bg.png", "https://abs.twimg.com/images/themes/theme1/bg.png", "https://pbs.twimg.com/profile_banners/1957558952/1452964462", "http://pbs.twimg.com/profile_images/667217829981106176/fovto_kT_normal.jpg", "https://pbs.twimg.com/profile_images/667217829981106176/fovto_kT_normal.jpg"),
          Seq("theguardian.com/environment/20\u2026", "http://www.theguardian.com/environment/2016/mar/30/atlantic-ocean-oil-gas-prospecting-marine-life-threats-obama", "https://t.co/1RkTeM0qsq", "greenpeace.org.uk/newsdesk", "http://www.greenpeace.org.uk/newsdesk", "http://t.co/LFhO8kD80W", "http://pbs.twimg.com/profile_background_images/4905693/n607755275_3319185_4102.jpg", "https://pbs.twimg.com/profile_background_images/4905693/n607755275_3319185_4102.jpg", "https://pbs.twimg.com/profile_banners/20056392/1435063529", "http://pbs.twimg.com/profile_images/626782894162374656/QfobfcAE_normal.jpg", "https://pbs.twimg.com/profile_images/626782894162374656/QfobfcAE_normal.jpg", "http://t.co/LFhO8kD80W"),
          Seq("apne.ws/1TiBHtW", "http://apne.ws/1TiBHtW", "https://t.co/AWOooi8HFX", "http://pbs.twimg.com/profile_background_images/378800000095494832/2bef35f4d3119d33ff0594560b242df6.jpeg", "https://pbs.twimg.com/profile_background_images/378800000095494832/2bef35f4d3119d33ff0594560b242df6.jpeg", "https://pbs.twimg.com/profile_banners/92585924/1429484924", "http://pbs.twimg.com/profile_images/707335559170031616/8x2Lcdlr_normal.jpg", "https://pbs.twimg.com/profile_images/707335559170031616/8x2Lcdlr_normal.jpg")
        )

        for (i <- 0 until df.count.toInt) {
          assert(actual(i)(0).equals(desired(i)))
        }
      }
    }
  }
}
