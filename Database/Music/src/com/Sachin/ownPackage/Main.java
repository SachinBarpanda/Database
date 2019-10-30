package com.Sachin.ownPackage;

import com.Sachin.ownPackage.model.Artist;
import com.Sachin.ownPackage.model.Datasource;
import com.Sachin.ownPackage.model.SongArtist;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        Datasource datasource = new Datasource();
        if(!datasource.open()){
            System.out.println("Couldn't open datasource");
            return;
        }
        List<Artist> artists = datasource.queryArtist(Datasource.ORDER_BY_NONE );
        if(artists==null){
            System.out.println("No artist");
            return;
        }
        for(Artist artist : artists){
            System.out.println("ID : "+ artist.getId()+" || Name : "+ artist.getName());
        }

        List<String> albumsForArtist =
                datasource.queryAlbumsForArtist("Carole King",Datasource.ORDER_BY_ASC);
        for(String albums : albumsForArtist){
            System.out.println(albums);
        }

        List<SongArtist> songArtist = datasource.queryArtistForSongs(
                "She's On Fire",Datasource.ORDER_BY_ASC);
        if(songArtist ==null){
            System.out.println("Couldn't Find the artist the song");
            return;
        }
        for(SongArtist artist : songArtist){
            System.out.println("Artist Name : "+ artist.getOnArtistName()+
                            "\nAlbum name  : "+ artist.getAlbumName()+
                            "\nTrack Name  : "+ artist.getTrack());
        }
        datasource.querySongsMetadata();
        int count = datasource.getCount(Datasource.TABLE_SONGS);
//        System.out.println("No of songs in the Table : "+ count);

        datasource.createViewForSongArtist();
        String title = "She's On Fire \"or 1=1 or\"";
        songArtist = datasource.querySongInfoView(title);

//        if(songArtist.isEmpty()){
//            System.out.println("Couldn't find the song info Table");
//            return;
//        }
        for(SongArtist artist : songArtist){
            System.out.println("FROM VIEW = "+artist.getOnArtistName()+
                    "Album Name : "+artist.getAlbumName() +
                    "Track Number : "+ artist.getTrack());
        }
        datasource.insertSong("Touch of Grey","GreatFul Dead","In the dark",1);
        datasource.close();
    }
}
